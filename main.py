import asyncio
import os
import logging
from typing import Dict, List, Optional
import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

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
            
            async with CACHE_LOCK:
                VEHICLES_BY_PATTERN = by_pattern
                VEHICLES_LAST_UPDATED = asyncio.get_event_loop().time()
            return True
        else:
            logger.error(f"Failed to fetch vehicles: HTTP {response.status_code}")
    except Exception as e:
        logger.error(f"Exception updating vehicles: {e}")
    return False

async def background_vehicle_poller():
    """Background task to poll vehicle positions every 10 seconds."""
    logger.info("Starting background vehicle poller...")
    async with httpx.AsyncClient() as client:
        while True:
            await update_vehicles_data(client)
            await asyncio.sleep(10.0)

async def get_pattern_data(pattern_id: str) -> Optional[dict]:
    """Retrieve pattern details, using cache if available."""
    if pattern_id in PATTERNS_CACHE:
        return PATTERNS_CACHE[pattern_id]

    async with httpx.AsyncClient() as client:
        try:
            logger.info(f"Fetching pattern {pattern_id} from API...")
            response = await client.get(f"{API_BASE_URL}/patterns/{pattern_id}", headers=HTTP_HEADERS, timeout=15.0)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    pattern = data[0]
                else:
                    pattern = data
                PATTERNS_CACHE[pattern_id] = pattern
                return pattern
            else:
                logger.error(f"Failed to fetch pattern {pattern_id}: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"Exception fetching pattern {pattern_id}: {e}")
    return None

@app.on_event("startup")
async def startup_event():
    """Load static datasets and launch background poller on server startup."""
    async with httpx.AsyncClient() as client:
        # Load core static datasets
        lines_success = await fetch_lines_data(client)
        stops_success = await fetch_stops_data(client)
        
        # Initial vehicle load
        await update_vehicles_data(client)
        
        # We start the background tasks
        asyncio.create_task(background_vehicle_poller())

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
            async with CACHE_LOCK:
                active_count = len(VEHICLES_BY_PATTERN.get(pid, []))
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


@app.get("/api/monitor")
async def monitor_pattern(
    pattern_id: str = Query(...),
    start_stop_id: Optional[str] = Query(None),
    end_stop_id: Optional[str] = Query(None)
):
    """Server-Sent Events endpoint to stream real-time updates for a monitored pattern and alert range."""
    
    async def event_generator():
        # Retrieve and check pattern
        pattern = await get_pattern_data(pattern_id)
        if not pattern:
            yield "event: error\ndata: {\"error\": \"Pattern not found\"}\n\n"
            return
            
        path = pattern.get("path", [])
        stop_seqs = {step["stop_id"]: step["stop_sequence"] for step in path}
        
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
                else:
                    is_in_alert_zone = False
                    stops_to_start = 0
                    stops_to_destination = -1
                
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

    return StreamingResponse(event_generator(), media_type="text/event-stream")

# Serve UI static files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/")
async def get_index():
    """Serve the single-page application frontend."""
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
