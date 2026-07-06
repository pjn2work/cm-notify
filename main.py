import asyncio
import os
import logging
from typing import Dict, List, Optional
import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import math
import time
from datetime import datetime
import aiosqlite

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bus-notifier")

app = FastAPI(title="Carris Metropolitana Bus Notifier")

# CORS middleware for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global caches
LINES_CACHE: List[dict] = []
STOPS_CACHE: Dict[str, dict] = {}
PATTERNS_CACHE: Dict[str, dict] = {}
SHAPES_CACHE: Dict[str, dict] = {}

# Vehicle tracking (updated by background task)
VEHICLES_BY_PATTERN: Dict[str, List[dict]] = {}
VEHICLES_LAST_UPDATED: float = 0.0
CACHE_LOCK = asyncio.Lock()
ACTIVE_SSE_CLIENTS: int = 0
DB_PATH = "stop_durations.db"
ALL_ADJACENT_PAIRS: set = set()   # (stop_from_id, stop_to_id) across all patterns
TRIP_SCHEDULES: Dict[str, Dict[str, int]] = {}  # trip_id -> {stop_id: seconds_since_midnight}
_PATTERN_BULK_LOAD_DONE: bool = False
VEHICLE_PREV_STOP: Dict[str, dict] = {}
PATTERNS_FETCH_QUEUED: set = set()
_PATTERN_FETCH_SEM: Optional[asyncio.Semaphore] = None

API_BASE_URL = "https://api.carrismetropolitana.pt/v2"
HTTP_HEADERS = {"User-Agent": "CarrisMetropolitanaBusNotifier/1.0"}

async def fetch_lines_data(client: httpx.AsyncClient) -> bool:
    global LINES_CACHE
    try:
        logger.info("Fetching lines from Carris Metropolitana API...")
        response = await client.get(f"{API_BASE_URL}/lines", headers=HTTP_HEADERS, timeout=20.0)
        if response.status_code == 200:
            LINES_CACHE = response.json()
            # Sort by short_name if it is a number
            def sort_key(line):
                sn = line.get("short_name", "")
                try:
                    return (0, int(sn))
                except ValueError:
                    return (1, sn)
            LINES_CACHE.sort(key=sort_key)
            logger.info(f"Successfully cached {len(LINES_CACHE)} lines.")
            return True
        else:
            logger.error(f"Failed to fetch lines: HTTP {response.status_code}")
    except Exception as e:
        logger.error(f"Exception fetching lines: {e}")
    return False

async def fetch_stops_data(client: httpx.AsyncClient) -> bool:
    global STOPS_CACHE
    try:
        logger.info("Fetching stops from Carris Metropolitana API...")
        response = await client.get(f"{API_BASE_URL}/stops", headers=HTTP_HEADERS, timeout=30.0)
        if response.status_code == 200:
            stops = response.json()
            STOPS_CACHE = {stop["id"]: stop for stop in stops}
            logger.info(f"Successfully cached {len(STOPS_CACHE)} stops.")
            return True
        else:
            logger.error(f"Failed to fetch stops: HTTP {response.status_code}")
    except Exception as e:
        logger.error(f"Exception fetching stops: {e}")
    return False

async def update_vehicles_data(client: httpx.AsyncClient) -> bool:
    global VEHICLES_BY_PATTERN, VEHICLES_LAST_UPDATED
    try:
        response = await client.get(f"{API_BASE_URL}/vehicles", headers=HTTP_HEADERS, timeout=15.0)
        if response.status_code == 200:
            vehicles = response.json()
            by_pattern = {}
            for v in vehicles:
                p_id = v.get("pattern_id")
                if p_id:
                    if p_id not in by_pattern:
                        by_pattern[p_id] = []
                    by_pattern[p_id].append(v)
            
            now = time.time()
            for v in vehicles:
                vid = v.get("id")
                curr_stop = v.get("stop_id")
                p_id = v.get("pattern_id")
                if not vid or not curr_stop or not p_id:
                    continue

                # Fetch any pattern we haven't seen yet — new buses starting shifts
                # bring new pattern IDs that must be cached immediately.
                if p_id not in PATTERNS_CACHE and p_id not in PATTERNS_FETCH_QUEUED:
                    PATTERNS_FETCH_QUEUED.add(p_id)
                    asyncio.create_task(_ensure_pattern_cached(p_id))

                status = v.get("current_status", "")
                prev = VEHICLE_PREV_STOP.get(vid)
                stop_changed = prev and prev["stop_id"] != curr_stop and prev["pattern_id"] == p_id

                if stop_changed and status == "STOPPED_AT":
                    # Bus has fully arrived at a new stop — record duration from when
                    # it first appeared at the previous stop to now.
                    duration = now - prev["timestamp"]
                    if (prev["stop_id"], curr_stop) in ALL_ADJACENT_PAIRS and 30 < duration < 1800:
                        asyncio.create_task(record_stop_transition(
                            prev["stop_id"], curr_stop, prev["timestamp"], duration
                        ))
                    VEHICLE_PREV_STOP[vid] = {"stop_id": curr_stop, "timestamp": now, "pattern_id": p_id}
                elif not prev or prev["pattern_id"] != p_id:
                    # First time seeing this vehicle (or it switched pattern): start tracking.
                    VEHICLE_PREV_STOP[vid] = {"stop_id": curr_stop, "timestamp": now, "pattern_id": p_id}
                # else: same stop (or not STOPPED_AT yet) → preserve first-seen timestamp

            async with CACHE_LOCK:
                VEHICLES_BY_PATTERN = by_pattern
                VEHICLES_LAST_UPDATED = asyncio.get_event_loop().time()
            return True
        else:
            logger.error(f"Failed to fetch vehicles: HTTP {response.status_code}")
    except Exception as e:
        logger.error(f"Exception updating vehicles: {e}")
    return False

async def background_pattern_refresher():
    """Clear pattern caches every 24 hours so schedules and routes stay current."""
    while True:
        await asyncio.sleep(24 * 3600)
        global _PATTERN_BULK_LOAD_DONE
        PATTERNS_CACHE.clear()
        PATTERNS_FETCH_QUEUED.clear()
        ALL_ADJACENT_PAIRS.clear()
        TRIP_SCHEDULES.clear()
        _PATTERN_BULK_LOAD_DONE = False
        logger.info("Pattern cache cleared — will repopulate on next vehicle poll.")


async def background_vehicle_poller():
    """Background task to poll vehicle positions every 10 seconds, only when clients are connected."""
    logger.info("Starting background vehicle poller...")
    async with httpx.AsyncClient() as client:
        while True:
            if ACTIVE_SSE_CLIENTS > 0:
                logger.info(f"[{ACTIVE_SSE_CLIENTS} client(s)] Polling vehicles...")
                await update_vehicles_data(client)
            await asyncio.sleep(10.0)

async def _ensure_pattern_cached(pattern_id: str):
    """Fetch and cache a pattern (including adjacency set) with concurrency limit."""
    global _PATTERN_BULK_LOAD_DONE
    async with _PATTERN_FETCH_SEM:
        if pattern_id not in PATTERNS_CACHE:
            if not _PATTERN_BULK_LOAD_DONE:
                done = sum(1 for p in PATTERNS_FETCH_QUEUED if p in PATTERNS_CACHE)
                total = len(PATTERNS_FETCH_QUEUED)
                pct = round(done / total * 100) if total else 0
                logger.info(f"Pattern cache: {done}/{total} ({pct}%) — fetching {pattern_id}")
            await get_pattern_data(pattern_id)

    if not _PATTERN_BULK_LOAD_DONE:
        done = sum(1 for p in PATTERNS_FETCH_QUEUED if p in PATTERNS_CACHE)
        total = len(PATTERNS_FETCH_QUEUED)
        if done >= total:
            _PATTERN_BULK_LOAD_DONE = True
            logger.info(f"Pattern cache complete: {total} patterns loaded.")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # Detect and drop old schema that included pattern_id in the primary key
        async with db.execute("PRAGMA table_info(stop_durations)") as cur:
            cols = [row[1] for row in await cur.fetchall()]
        if "pattern_id" in cols:
            logger.info("Migrating stop_durations: dropping pattern_id-keyed schema.")
            await db.execute("DROP TABLE stop_durations")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stop_durations (
                stop_from_id TEXT,
                stop_to_id TEXT,
                day_of_week INTEGER,
                hour INTEGER,
                avg_seconds REAL,
                min_seconds REAL,
                max_seconds REAL,
                sample_count INTEGER,
                PRIMARY KEY (stop_from_id, stop_to_id, day_of_week, hour)
            )
        """)
        await db.commit()
    logger.info("SQLite ETA database initialized.")


async def record_stop_transition(stop_from: str, stop_to: str, timestamp: float, duration_seconds: float):
    dt = datetime.fromtimestamp(timestamp)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO stop_durations (stop_from_id, stop_to_id, day_of_week, hour, avg_seconds, min_seconds, max_seconds, sample_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(stop_from_id, stop_to_id, day_of_week, hour) DO UPDATE SET
                avg_seconds = (avg_seconds * sample_count + excluded.avg_seconds) / (sample_count + 1),
                min_seconds = MIN(COALESCE(min_seconds, excluded.min_seconds), excluded.min_seconds),
                max_seconds = MAX(COALESCE(max_seconds, excluded.max_seconds), excluded.max_seconds),
                sample_count = sample_count + 1
        """, (stop_from, stop_to, dt.weekday(), dt.hour, duration_seconds, duration_seconds, duration_seconds))
        await db.commit()


async def get_pattern_historical_data(pattern_id: str, day: int, hour: int) -> dict:
    pattern = PATTERNS_CACHE.get(pattern_id)
    if not pattern:
        return {}
    path = sorted(pattern.get("path", []), key=lambda s: s["stop_sequence"])
    pair_keys = [f"{path[i]['stop_id']}__{path[i+1]['stop_id']}" for i in range(len(path) - 1)]
    if not pair_keys:
        return {}
    placeholders = ",".join(["?"] * len(pair_keys))
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"""
            SELECT stop_from_id, stop_to_id, avg_seconds, min_seconds, max_seconds, sample_count
            FROM stop_durations
            WHERE day_of_week = ? AND hour = ?
            AND stop_from_id || '__' || stop_to_id IN ({placeholders})
        """, [day, hour] + pair_keys) as cursor:
            rows = await cursor.fetchall()
    return {
        (row[0], row[1]): {
            "avg_seconds": row[2], "min_seconds": row[3],
            "max_seconds": row[4], "sample_count": row[5]
        }
        for row in rows
    }


def _parse_time_s(t: str) -> int:
    """Parse HH:MM:SS (including >24h overnight values) to seconds since midnight."""
    h, m, s = t.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres between two GPS coordinates."""
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def calculate_eta_seconds(
    path_ordered: list, from_seq: int, to_seq: int,
    speed_ms: float, hist_data: dict,
    trip_schedule: Optional[dict] = None
) -> float:
    """ETA in seconds. Priority per segment:
      1. 50% schedule + 50% historical  (both available)
      2. Schedule only
      3. Historical only
      4. Route distance / speed fallback
    speed_ms is in m/s (as returned by the Carris vehicles API)."""
    if from_seq >= to_seq:
        return 0.0
    speed_ms = max(speed_ms or 0, 10 / 3.6)  # floor at 10 km/h in m/s
    steps = [s for s in path_ordered if from_seq <= s["stop_sequence"] <= to_seq]
    total = 0.0
    for i in range(len(steps) - 1):
        sf, st = steps[i], steps[i + 1]

        scheduled = None
        if trip_schedule:
            t_from = trip_schedule.get(sf["stop_id"])
            t_to = trip_schedule.get(st["stop_id"])
            if t_from is not None and t_to is not None and t_to > t_from:
                scheduled = float(t_to - t_from)

        hist = hist_data.get((sf["stop_id"], st["stop_id"]))
        historical = hist["avg_seconds"] if hist else None

        if scheduled is not None and historical is not None:
            segment_eta = 0.5 * scheduled + 0.5 * historical
        elif scheduled is not None:
            segment_eta = scheduled
        elif historical is not None:
            segment_eta = historical
        else:
            raw_km = st.get("distance", 0) - sf.get("distance", 0)
            if raw_km > 0:
                seg_dist = raw_km * 1000
            else:
                lat1, lon1 = sf.get("lat"), sf.get("lon")
                lat2, lon2 = st.get("lat"), st.get("lon")
                seg_dist = (_haversine_m(lat1, lon1, lat2, lon2)
                            if lat1 and lon1 and lat2 and lon2 else 300.0)
            segment_eta = seg_dist / speed_ms

        total += segment_eta
    return total


async def get_pattern_data(pattern_id: str) -> Optional[dict]:
    """Retrieve pattern details, using cache if available."""
    if pattern_id in PATTERNS_CACHE:
        return PATTERNS_CACHE[pattern_id]

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{API_BASE_URL}/patterns/{pattern_id}", headers=HTTP_HEADERS, timeout=15.0)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    pattern = data[0]
                else:
                    pattern = data
                PATTERNS_CACHE[pattern_id] = pattern
                sorted_path = sorted(pattern.get("path", []), key=lambda s: s["stop_sequence"])
                for i in range(len(sorted_path) - 1):
                    ALL_ADJACENT_PAIRS.add((sorted_path[i]["stop_id"], sorted_path[i + 1]["stop_id"]))
                for trip in pattern.get("trips", []):
                    sched = {
                        entry["stop_id"]: _parse_time_s(entry["arrival_time_24h"])
                        for entry in trip.get("schedule", [])
                        if entry.get("arrival_time_24h")
                    }
                    for tid in trip.get("trip_ids", []):
                        TRIP_SCHEDULES[tid] = sched
                return pattern
            else:
                logger.error(f"Failed to fetch pattern {pattern_id}: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"Exception fetching pattern {pattern_id}: {e}")
    return None

@app.on_event("startup")
async def startup_event():
    """Load static datasets and launch background poller on server startup."""
    global _PATTERN_FETCH_SEM
    _PATTERN_FETCH_SEM = asyncio.Semaphore(5)
    await init_db()
    async with httpx.AsyncClient() as client:
        # Load core static datasets
        lines_success = await fetch_lines_data(client)
        stops_success = await fetch_stops_data(client)
        
        # Initial vehicle load
        await update_vehicles_data(client)
        
        # We start the background tasks
        asyncio.create_task(background_vehicle_poller())
        asyncio.create_task(background_pattern_refresher())

@app.get("/api/lines")
async def get_lines(search: Optional[str] = None):
    """Retrieve list of lines, with optional query filtering."""
    if not LINES_CACHE:
        # Fallback if cache is empty
        async with httpx.AsyncClient() as client:
            await fetch_lines_data(client)

    if not search:
        return LINES_CACHE[:100]  # Limit initial response for speed

    query = search.lower()
    filtered = [
        line for line in LINES_CACHE
        if query in line.get("short_name", "").lower() or query in line.get("long_name", "").lower()
    ]
    return filtered[:50]  # Limit search results

@app.get("/api/lines/{line_id}/patterns")
async def get_line_patterns(line_id: str):
    """Get patterns (directions) for a specific line."""
    if not LINES_CACHE:
        async with httpx.AsyncClient() as client:
            await fetch_lines_data(client)

    line = next((l for l in LINES_CACHE if l["id"] == line_id), None)
    if not line:
        # Try searching by short_name
        line = next((l for l in LINES_CACHE if l["short_name"] == line_id), None)
        if not line:
            raise HTTPException(status_code=404, detail="Line not found")

    pattern_ids = line.get("pattern_ids", [])
    patterns = []
    
    for pid in pattern_ids:
        p_data = await get_pattern_data(pid)
        if p_data:
            pattern_stop_ids = {step["stop_id"] for step in p_data.get("path", [])}
            async with CACHE_LOCK:
                vehicles_for_pattern = VEHICLES_BY_PATTERN.get(pid, [])
            active_count = sum(
                1 for v in vehicles_for_pattern
                if v.get("stop_id") in pattern_stop_ids
            )
            patterns.append({
                "id": p_data.get("id"),
                "headsign": p_data.get("headsign"),
                "direction_id": p_data.get("direction_id"),
                "color": p_data.get("color"),
                "text_color": p_data.get("text_color"),
                "stop_count": len(p_data.get("path", [])),
                "active_bus_count": active_count
            })
            
    # Sort directions
    patterns.sort(key=lambda x: x.get("direction_id", 0))
    return patterns

@app.get("/api/patterns/{pattern_id}")
async def get_pattern(pattern_id: str):
    """Get full details of a pattern, including stop details merged from static stop cache."""
    pattern = await get_pattern_data(pattern_id)
    if not pattern:
        raise HTTPException(status_code=404, detail="Pattern not found")
        
    path = []
    for step in pattern.get("path", []):
        stop_id = step["stop_id"]
        stop_info = STOPS_CACHE.get(stop_id, {})
        path.append({
            "stop_id": stop_id,
            "stop_sequence": step["stop_sequence"],
            "allow_pickup": step.get("allow_pickup", True),
            "allow_drop_off": step.get("allow_drop_off", True),
            "distance": step.get("distance", 0),
            "name": stop_info.get("long_name", f"Stop {stop_id}"),
            "lat": stop_info.get("lat"),
            "lon": stop_info.get("lon"),
            "facilities": stop_info.get("facilities", [])
        })
        
    return {
        "id": pattern.get("id"),
        "line_id": pattern.get("line_id"),
        "headsign": pattern.get("headsign"),
        "direction_id": pattern.get("direction_id"),
        "color": pattern.get("color", "#000000"),
        "text_color": pattern.get("text_color", "#ffffff"),
        "shape_id": pattern.get("shape_id"),
        "path": path
    }

@app.get("/api/shapes/{shape_id:path}")
async def get_shape(shape_id: str):
    """Proxy and cache route shape geometry from the Carris Metropolitana API."""
    if shape_id in SHAPES_CACHE:
        return SHAPES_CACHE[shape_id]

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{API_BASE_URL}/shapes/{shape_id}",
                headers=HTTP_HEADERS,
                timeout=15.0
            )
            if response.status_code == 200:
                data = response.json()
                SHAPES_CACHE[shape_id] = data
                return data
            else:
                raise HTTPException(status_code=response.status_code, detail="Shape not found")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Exception fetching shape {shape_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/patterns/{pattern_id}/durations")
async def get_pattern_durations(pattern_id: str):
    """Aggregated historical stop-pair durations for a pattern across all day/hour slots."""
    pattern = PATTERNS_CACHE.get(pattern_id) or await get_pattern_data(pattern_id)
    if not pattern:
        return {}
    path = sorted(pattern.get("path", []), key=lambda s: s["stop_sequence"])
    pair_keys = [f"{path[i]['stop_id']}__{path[i+1]['stop_id']}" for i in range(len(path) - 1)]
    if not pair_keys:
        return {}
    placeholders = ",".join(["?"] * len(pair_keys))
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"""
            SELECT stop_from_id, stop_to_id,
                SUM(avg_seconds * sample_count) / SUM(sample_count) AS overall_avg,
                MIN(min_seconds) AS overall_min,
                MAX(max_seconds) AS overall_max,
                SUM(sample_count) AS total_samples
            FROM stop_durations
            WHERE stop_from_id || '__' || stop_to_id IN ({placeholders})
            GROUP BY stop_from_id, stop_to_id
        """, pair_keys) as cursor:
            rows = await cursor.fetchall()
    return {
        f"{row[0]}__{row[1]}": {
            "avg_seconds": row[2], "min_seconds": row[3],
            "max_seconds": row[4], "sample_count": int(row[5])
        }
        for row in rows
    }


@app.get("/api/patterns/{pattern_id}/heatmap")
async def get_pattern_heatmap(pattern_id: str, day: int = Query(..., ge=0, le=6)):
    """Per-hour average duration for each stop pair on a given day of week."""
    pattern = PATTERNS_CACHE.get(pattern_id) or await get_pattern_data(pattern_id)
    if not pattern:
        return {}
    path = sorted(pattern.get("path", []), key=lambda s: s["stop_sequence"])
    pair_keys = [f"{path[i]['stop_id']}__{path[i+1]['stop_id']}" for i in range(len(path) - 1)]
    if not pair_keys:
        return {}
    placeholders = ",".join(["?"] * len(pair_keys))
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"""
            SELECT stop_from_id, stop_to_id, hour, avg_seconds, sample_count
            FROM stop_durations
            WHERE day_of_week = ?
            AND stop_from_id || '__' || stop_to_id IN ({placeholders})
        """, [day] + pair_keys) as cursor:
            rows = await cursor.fetchall()
    result = {}
    for stop_from, stop_to, hour, avg_sec, samples in rows:
        key = f"{stop_from}__{stop_to}"
        if key not in result:
            result[key] = {}
        result[key][hour] = {"avg_seconds": avg_sec, "sample_count": samples}
    return result


@app.get("/api/monitor")
async def monitor_pattern(
    pattern_id: str = Query(...),
    start_stop_id: Optional[str] = Query(None),
    end_stop_id: Optional[str] = Query(None)
):
    """Server-Sent Events endpoint to stream real-time updates for a monitored pattern and alert range."""
    
    async def event_generator():
        global ACTIVE_SSE_CLIENTS
        ACTIVE_SSE_CLIENTS += 1
        try:
            # Retrieve and check pattern
            pattern = await get_pattern_data(pattern_id)
            if not pattern:
                yield "event: error\ndata: {\"error\": \"Pattern not found\"}\n\n"
                return

            path = pattern.get("path", [])
            stop_seqs = {step["stop_id"]: step["stop_sequence"] for step in path}
            # Enrich with stop coordinates so haversine can compute accurate distances
            path_ordered = sorted(
                [
                    {**step,
                     "lat": STOPS_CACHE.get(step["stop_id"], {}).get("lat"),
                     "lon": STOPS_CACHE.get(step["stop_id"], {}).get("lon")}
                    for step in path
                ],
                key=lambda s: s["stop_sequence"]
            )

            has_alert_zone = start_stop_id is not None and end_stop_id is not None
            start_seq = None
            end_seq = None
            start_stop_id_real = None
            end_stop_id_real = None

            if has_alert_zone:
                start_seq = stop_seqs.get(start_stop_id)
                end_seq = stop_seqs.get(end_stop_id)

                if start_seq is None or end_seq is None:
                    yield "event: error\ndata: {\"error\": \"Start or Destination stop not found in pattern path\"}\n\n"
                    return

                # Standardize range: start_seq must be less than or equal to end_seq
                if start_seq > end_seq:
                    # If swapped, swap them back
                    start_seq, end_seq = end_seq, start_seq
                    start_stop_id_real, end_stop_id_real = end_stop_id, start_stop_id
                else:
                    start_stop_id_real, end_stop_id_real = start_stop_id, end_stop_id

                logger.info(f"Started monitoring SSE connection: pattern={pattern_id}, alert_zone=[{start_seq} ({start_stop_id_real}) -> {end_seq} ({end_stop_id_real})]")
            else:
                logger.info(f"Started preview SSE connection: pattern={pattern_id}")

            # SSE keep-alive header
            yield "comment: connection established\n\n"

            while True:
                # Read from memory-cached vehicle locations
                async with CACHE_LOCK:
                    vehicles = VEHICLES_BY_PATTERN.get(pattern_id, [])
                    last_updated = VEHICLES_LAST_UPDATED

                now_dt = datetime.now()
                hist_data = await get_pattern_historical_data(pattern_id, now_dt.weekday(), now_dt.hour)

                monitored_buses = []
                for v in vehicles:
                    stop_id = v.get("stop_id")
                    current_seq = stop_seqs.get(stop_id)

                    # If vehicle stop is not on pattern path, skip or fallback to sequence
                    if current_seq is None:
                        continue

                    if has_alert_zone:
                        is_in_alert_zone = start_seq <= current_seq <= end_seq
                        stops_to_start = start_seq - current_seq if current_seq < start_seq else 0
                        stops_to_destination = end_seq - current_seq if current_seq <= end_seq else -1
                        speed = v.get("speed") or 0  # m/s from Carris API
                        trip_schedule = TRIP_SCHEDULES.get(v.get("trip_id", ""))
                        eta_to_start_seconds = (
                            calculate_eta_seconds(path_ordered, current_seq, start_seq, speed, hist_data, trip_schedule)
                            if current_seq < start_seq else None
                        )
                        eta_to_end_seconds = (
                            calculate_eta_seconds(path_ordered, current_seq, end_seq, speed, hist_data, trip_schedule)
                            if current_seq <= end_seq else None
                        )
                    else:
                        is_in_alert_zone = False
                        stops_to_start = 0
                        stops_to_destination = -1
                        eta_to_start_seconds = None
                        eta_to_end_seconds = None

                    stop_info = STOPS_CACHE.get(stop_id, {})
                    stop_name = stop_info.get("long_name", f"Stop {stop_id}")

                    monitored_buses.append({
                        "vehicle_id": v.get("id"),
                        "license_plate": v.get("license_plate", "Unknown"),
                        "lat": v.get("lat"),
                        "lon": v.get("lon"),
                        "speed": v.get("speed", 0),
                        "bearing": v.get("bearing", 0),
                        "current_stop_id": stop_id,
                        "current_stop_name": stop_name,
                        "current_stop_sequence": current_seq,
                        "current_status": v.get("current_status", "UNKNOWN"),
                        "is_in_alert_zone": is_in_alert_zone,
                        "stops_to_start": stops_to_start,
                        "stops_to_destination": stops_to_destination,
                        "eta_to_start_seconds": round(eta_to_start_seconds) if eta_to_start_seconds is not None else None,
                        "eta_to_end_seconds": round(eta_to_end_seconds) if eta_to_end_seconds is not None else None,
                        "make": v.get("make", ""),
                        "model": v.get("model", ""),
                        "owner": v.get("owner", ""),
                        "shift_id": v.get("shift_id", ""),
                        "propulsion": v.get("propulsion", ""),
                        "bikes_allowed": v.get("bikes_allowed", ""),
                        "capacity_total": v.get("capacity_total", ""),
                        "contactless": v.get("contactless", ""),
                        "wheelchair_accessible": v.get("wheelchair_accessible", ""),
                    })

                import json
                payload = {
                    "pattern_id": pattern_id,
                    "start_stop_id": start_stop_id_real,
                    "start_stop_sequence": start_seq,
                    "end_stop_id": end_stop_id_real,
                    "end_stop_sequence": end_seq,
                    "buses": monitored_buses,
                    "last_updated": last_updated
                }

                yield f"data: {json.dumps(payload)}\n\n"

                # Sleep 5 seconds before checking again
                await asyncio.sleep(5.0)
        finally:
            ACTIVE_SSE_CLIENTS -= 1

    return StreamingResponse(event_generator(), media_type="text/event-stream")

# Serve UI static files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/")
async def get_index():
    """Serve the single-page application frontend."""
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
