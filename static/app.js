// State Management
let selectedLine = null;
let selectedPattern = null;
let patternPath = [];
let startStopId = null;
let startStopSeq = null;
let startStopName = "";
let endStopId = null;
let endStopSeq = null;
let endStopName = "";

let segmentDurations = {};
let durationRefreshInterval = null;
let heatmapData = {};

let eventSource = null;
let previewEventSource = null;
let alertedVehicles = new Set();
let isMonitoring = false;

// Map state
let map = null;
let busMapMarkers = {};
let routePolyline = null;
let routeStopMarkers = {}; // keyed by stop_id
let currentView = 'timeline';
let patternShape = null; // GeoJSON shape geometry for the selected pattern

// Audio Context for sound alerts
let audioCtx = null;

// Track which bus tooltips are expanded (persists across SSE refreshes)
let expandedBuses = new Set();

// Initialize Elements
document.addEventListener("DOMContentLoaded", () => {
    initLineSearch();
    initNotificationSettings();
    initStatusCheck();
    document.getElementById("timelineViewBtn").addEventListener("click", () => setView('timeline'));
    document.getElementById("mapViewBtn").addEventListener("click", () => setView('map'));
    document.getElementById("statsViewBtn").addEventListener("click", () => setView('stats'));
    initVisibilityHandling();
});

// Pause SSE when tab is hidden, resume when visible again
function initVisibilityHandling() {
    document.addEventListener("visibilitychange", () => {
        if (document.hidden) {
            // Tab hidden — close all SSE connections to stop server polling
            stopPreview();
            if (eventSource) {
                eventSource.close();
                eventSource = null;
            }
        } else {
            // Tab visible again — reconnect whichever stream was active
            if (isMonitoring && selectedPattern) {
                const url = `/api/monitor?pattern_id=${encodeURIComponent(selectedPattern.id)}&start_stop_id=${encodeURIComponent(startStopId)}&end_stop_id=${encodeURIComponent(endStopId)}`;
                eventSource = new EventSource(url);
                eventSource.onmessage = (event) => {
                    const payload = JSON.parse(event.data);
                    handleMonitoringData(payload);
                };
                eventSource.onerror = (err) => {
                    console.error("SSE connection error:", err);
                    document.getElementById("activeBusesCount").textContent = "Reconnecting stream...";
                };
            } else if (selectedPattern && !isMonitoring) {
                startPreview(selectedPattern.id);
            }
        }
    });
}

// Check API Connection Status
async function initStatusCheck() {
    const statusDot = document.getElementById("statusDot");
    const statusText = document.getElementById("statusText");
    
    try {
        const res = await fetch("/api/lines");
        if (res.ok) {
            statusDot.className = "status-indicator online";
            statusText.textContent = "API Live & Ready";
        } else {
            statusDot.className = "status-indicator offline";
            statusText.textContent = "API Error";
        }
    } catch (e) {
        statusDot.className = "status-indicator offline";
        statusText.textContent = "Server Offline";
    }
}

// Line Search Autocomplete
function initLineSearch() {
    const input = document.getElementById("lineInput");
    const list = document.getElementById("autocompleteList");
    const clearBtn = document.getElementById("clearLineBtn");
    
    let debounceTimer;
    
    input.addEventListener("input", (e) => {
        clearTimeout(debounceTimer);
        const query = e.target.value.trim();
        
        if (!query) {
            list.style.display = "none";
            clearBtn.style.display = "none";
            return;
        }
        
        clearBtn.style.display = "block";
        
        debounceTimer = setTimeout(async () => {
            try {
                const res = await fetch(`/api/lines?search=${encodeURIComponent(query)}`);
                const lines = await res.json();
                renderAutocomplete(lines);
            } catch (err) {
                console.error("Error fetching lines:", err);
            }
        }, 200);
    });
    
    clearBtn.addEventListener("click", () => {
        input.value = "";
        list.style.display = "none";
        clearBtn.style.display = "none";
        resetApp();
    });
    
    // Close list when clicking outside
    document.addEventListener("click", (e) => {
        if (e.target !== input && e.target !== list) {
            list.style.display = "none";
        }
    });
}

function renderAutocomplete(lines) {
    const list = document.getElementById("autocompleteList");
    list.innerHTML = "";
    
    if (lines.length === 0) {
        list.style.display = "none";
        return;
    }
    
    lines.forEach(line => {
        const item = document.createElement("div");
        item.className = "autocomplete-item";
        
        const badge = document.createElement("span");
        badge.className = "line-badge";
        badge.style.backgroundColor = line.color || "#1e293b";
        badge.style.color = line.text_color || "#ffffff";
        badge.textContent = line.short_name;
        
        const name = document.createElement("span");
        name.className = "line-name";
        name.textContent = line.long_name;
        
        item.appendChild(badge);
        item.appendChild(name);
        
        item.addEventListener("click", () => {
            selectLine(line);
        });
        
        list.appendChild(item);
    });
    
    list.style.display = "block";
}

function selectLine(line) {
    selectedLine = line;
    const input = document.getElementById("lineInput");
    input.value = `${line.short_name} - ${line.long_name}`;
    document.getElementById("autocompleteList").style.display = "none";
    
    // Reset settings
    resetMonitoringState();
    selectedPattern = null;
    patternPath = [];
    patternShape = null;
    clearMapRoute();
    clearMapBuses();
    document.getElementById("welcomeContainer").style.display = "flex";
    document.getElementById("monitorDashboard").style.display = "none";
    document.getElementById("alertConfigDetails").style.display = "none";
    
    // Fetch patterns (directions)
    loadDirections(line.id);
}

// Load Patterns/Directions
async function loadDirections(lineId) {
    const container = document.getElementById("directionOptions");
    const group = document.getElementById("directionGroup");
    container.innerHTML = `<div class="dir-meta">Loading directions...</div>`;
    group.style.display = "block";
    
    try {
        const res = await fetch(`/api/lines/${lineId}/patterns`);
        const patterns = await res.json();
        
        container.innerHTML = "";
        patterns.forEach(pat => {
            const card = document.createElement("div");
            card.className = "direction-card";
            card.dataset.patternId = pat.id;
            
            const busCount = pat.active_bus_count || 0;
            const busText = busCount === 1 ? "1 active bus" : `${busCount} active buses`;
            card.innerHTML = `
                <div class="dir-title">to ${pat.headsign}</div>
                <div class="dir-meta">${pat.stop_count} stops • ${busText}</div>
            `;
            
            card.addEventListener("click", () => {
                // Remove previous selected
                document.querySelectorAll(".direction-card").forEach(c => c.classList.remove("selected"));
                card.classList.add("selected");
                selectPattern(pat);
            });
            
            container.appendChild(card);
        });
    } catch (err) {
        container.innerHTML = `<div class="dir-meta" style="color: var(--accent-red)">Error loading directions</div>`;
    }
}

// Select Pattern
async function selectPattern(pattern) {
    selectedPattern = pattern;
    resetMonitoringState();
    
    try {
        const res = await fetch(`/api/patterns/${pattern.id}`);
        const data = await res.json();
        patternPath = data.path;

        // Fetch historical segment durations and keep them refreshed
        segmentDurations = {};
        try {
            const durRes = await fetch(`/api/patterns/${pattern.id}/durations`);
            if (durRes.ok) segmentDurations = await durRes.json();
        } catch (e) { /* non-critical */ }
        clearInterval(durationRefreshInterval);
        durationRefreshInterval = setInterval(refreshSegmentDurations, 30000);

        // Fetch shape geometry for accurate map route rendering
        patternShape = null;
        if (data.shape_id) {
            try {
                const shapeRes = await fetch(`/api/shapes/${encodeURIComponent(data.shape_id)}`);
                if (shapeRes.ok) patternShape = await shapeRes.json();
            } catch (e) {
                console.warn("Could not fetch shape geometry:", e);
            }
        }

        // Show monitoring panels
        document.getElementById("welcomeContainer").style.display = "none";
        document.getElementById("monitorDashboard").style.display = "flex";
        document.getElementById("alertConfigDetails").style.display = "block";
        
        // Update header badges
        const colorBadge = document.getElementById("badgeLineColor");
        colorBadge.style.backgroundColor = data.color || "#4f46e5";
        
        const lineText = document.getElementById("badgeLineText");
        lineText.textContent = `Line ${selectedLine.short_name} to ${data.headsign}`;
        
        // Render Timeline
        renderTimeline(patternPath);
        if (currentView === 'stats') fetchAndRenderHeatmap();

        // Draw route on map if already initialized
        if (map) drawRouteOnMap();

        // Start preview SSE to show live bus positions immediately
        startPreview(pattern.id);
        
    } catch (err) {
        console.error("Error loading pattern stops:", err);
    }
}

// Render Timeline stops
function renderTimeline(path) {
    const container = document.getElementById("routeTimeline");
    container.innerHTML = "";
    
    // Add glow track layer
    const glowTrack = document.createElement("div");
    glowTrack.className = "vertical-timeline-track-glow";
    glowTrack.id = "timelineGlowTrack";
    glowTrack.style.display = "none";
    container.appendChild(glowTrack);
    
    path.forEach((step, idx) => {
        const node = document.createElement("div");
        node.className = "timeline-node";
        node.dataset.stopId = step.stop_id;
        node.dataset.sequence = step.stop_sequence;
        node.dataset.index = idx;
        
        node.innerHTML = `
            <div class="timeline-node-circle"></div>
            <div class="timeline-node-content">
                <span class="stop-name">${step.name}</span>
                <span class="stop-seq">Stop ${step.stop_sequence} • ID: ${step.stop_id}</span>
            </div>
            <div class="timeline-node-actions">
                <button class="action-btn set-start">Alert Start</button>
                <button class="action-btn set-end">Destination</button>
            </div>
        `;
        
        // Set events on action buttons
        node.querySelector(".set-start").addEventListener("click", (e) => {
            e.stopPropagation();
            setAlertStart(step.stop_id, step.stop_sequence, step.name);
        });
        node.querySelector(".set-end").addEventListener("click", (e) => {
            e.stopPropagation();
            setAlertEnd(step.stop_id, step.stop_sequence, step.name);
        });
        
        container.appendChild(node);

        // Segment info strip between this stop and the next
        if (idx < path.length - 1) {
            const nextStep = path[idx + 1];
            const distM = Math.round((nextStep.distance - step.distance) * 1000);
            const distText = distM >= 1000
                ? `${(distM / 1000).toFixed(1)} km`
                : distM > 0 ? `${distM} m` : null;

            const key = `${step.stop_id}__${nextStep.stop_id}`;
            const seg = segmentDurations[key];
            const hasSeg = seg && seg.sample_count > 0;

            if (distText || hasSeg) {
                const segEl = document.createElement("div");
                segEl.className = "timeline-segment-info";
                segEl.dataset.segKey = key;
                segEl.innerHTML = `
                    ${distText ? `<span class="seg-dist">${distText}</span>` : ""}
                    ${hasSeg ? `<span class="seg-avg">⌀ ${formatDuration(seg.avg_seconds)}</span>` : ""}
                    ${hasSeg ? `<span class="seg-range">${formatDuration(seg.min_seconds)}–${formatDuration(seg.max_seconds)} · ${seg.sample_count}</span>` : ""}
                `;
                container.appendChild(segEl);
            }
        }
    });

    updateTimelineHighlight();
}

// Set Alert Boundaries
function setAlertStart(stopId, sequence, name) {
    if (endStopSeq !== null && sequence >= endStopSeq) {
        alert("Alert Start stop must be positioned BEFORE your Destination stop.");
        return;
    }
    
    startStopId = stopId;
    startStopSeq = sequence;
    startStopName = name;
    
    const startCard = document.querySelector(".stop-summary-card.start");
    startCard.classList.add("filled");
    document.getElementById("startStopVal").textContent = `${name} (Stop ${sequence})`;
    
    updateTimelineHighlight();
    validateMonitoringButton();
    refreshMapStopColors();
}

function setAlertEnd(stopId, sequence, name) {
    if (startStopSeq !== null && sequence <= startStopSeq) {
        alert("Destination stop must be positioned AFTER your Alert Start stop.");
        return;
    }
    
    endStopId = stopId;
    endStopSeq = sequence;
    endStopName = name;
    
    const endCard = document.querySelector(".stop-summary-card.end");
    endCard.classList.add("filled");
    document.getElementById("endStopVal").textContent = `${name} (Stop ${sequence})`;
    
    updateTimelineHighlight();
    validateMonitoringButton();
    refreshMapStopColors();
}

// Update Timeline Highlight Graphics
function updateTimelineHighlight() {
    const glowTrack = document.getElementById("timelineGlowTrack");
    const nodes = document.querySelectorAll(".timeline-node");
    
    nodes.forEach(node => {
        node.classList.remove("in-alert-zone", "alert-start", "destination");
    });
    
    if (!startStopId || !endStopId) {
        if (glowTrack) glowTrack.style.display = "none";
        return;
    }
    
    const startNode = document.querySelector(`.timeline-node[data-stop-id="${startStopId}"]`);
    const endNode = document.querySelector(`.timeline-node[data-stop-id="${endStopId}"]`);
    
    if (startNode && endNode) {
        const startIndex = parseInt(startNode.dataset.index);
        const endIndex = parseInt(endNode.dataset.index);
        
        for (let i = startIndex; i <= endIndex; i++) {
            const node = document.querySelector(`.timeline-node[data-index="${i}"]`);
            if (node) {
                if (i === startIndex) {
                    node.classList.add("alert-start");
                } else if (i === endIndex) {
                    node.classList.add("destination");
                } else {
                    node.classList.add("in-alert-zone");
                }
            }
        }
        
        if (glowTrack) {
            const startTop = startNode.offsetTop + 10;
            const endTop = endNode.offsetTop + 10;
            glowTrack.style.top = `${startTop}px`;
            glowTrack.style.height = `${endTop - startTop}px`;
            glowTrack.style.display = "block";
        }
    }
}

function validateMonitoringButton() {
    const btn = document.getElementById("startMonitorBtn");
    btn.disabled = !(startStopId && endStopId && selectedPattern);
}

// Reset functions
function resetApp() {
    selectedLine = null;
    selectedPattern = null;
    patternPath = [];
    segmentDurations = {};
    heatmapData = {};
    setView('timeline');
    clearInterval(durationRefreshInterval);
    durationRefreshInterval = null;
    resetMonitoringState();
    
    document.getElementById("directionGroup").style.display = "none";
    document.getElementById("alertConfigDetails").style.display = "none";
    document.getElementById("welcomeContainer").style.display = "flex";
    document.getElementById("monitorDashboard").style.display = "none";
}

function resetMonitoringState() {
    stopMonitoring();
    stopPreview();
    startStopId = null;
    startStopSeq = null;
    startStopName = "";
    endStopId = null;
    endStopSeq = null;
    endStopName = "";
    
    const startCard = document.querySelector(".stop-summary-card.start");
    startCard.classList.remove("filled");
    document.getElementById("startStopVal").textContent = "Select on route map below";
    
    const endCard = document.querySelector(".stop-summary-card.end");
    endCard.classList.remove("filled");
    document.getElementById("endStopVal").textContent = "Select on route map below";
    
    document.getElementById("startMonitorBtn").disabled = true;
}

// Notification Permissions
function initNotificationSettings() {
    const toggle = document.getElementById("notificationToggle");
    const modal = document.getElementById("notificationModal");
    const enableBtn = document.getElementById("modalEnableBtn");
    const dismissBtn = document.getElementById("modalDismissBtn");
    
    // Sync UI with state
    if (Notification.permission === "granted") {
        toggle.checked = true;
    } else {
        toggle.checked = false;
    }
    
    toggle.addEventListener("change", (e) => {
        if (e.target.checked) {
            if (Notification.permission === "default") {
                modal.classList.add("open");
            } else if (Notification.permission === "denied") {
                alert("Desktop notifications are blocked by your browser settings. Please enable them in your address bar permissions.");
                toggle.checked = false;
            }
        }
    });
    
    enableBtn.addEventListener("click", async () => {
        modal.classList.remove("open");
        const permission = await Notification.requestPermission();
        if (permission === "granted") {
            toggle.checked = true;
        } else {
            toggle.checked = false;
        }
    });
    
    dismissBtn.addEventListener("click", () => {
        modal.classList.remove("open");
        toggle.checked = false;
    });
    
    // Bind Start and Stop buttons
    document.getElementById("startMonitorBtn").addEventListener("click", startMonitoring);
    document.getElementById("stopMonitorBtn").addEventListener("click", stopMonitoring);
}

// Chime generator using Web Audio API
function playChime() {
    const playSound = document.getElementById("soundToggle").checked;
    if (!playSound) return;
    
    try {
        if (!audioCtx) {
            audioCtx = new (window.AudioContext || window.webkitAudioContext)();
        }
        
        if (audioCtx.state === "suspended") {
            audioCtx.resume();
        }
        
        const now = audioCtx.currentTime;
        const osc1 = audioCtx.createOscillator();
        const osc2 = audioCtx.createOscillator();
        const gain = audioCtx.createGain();
        
        osc1.type = "sine";
        osc1.frequency.setValueAtTime(554.37, now); // C#5
        osc1.frequency.setValueAtTime(659.25, now + 0.12); // E5
        
        osc2.type = "triangle";
        osc2.frequency.setValueAtTime(369.99, now); // F#4
        osc2.frequency.setValueAtTime(554.37, now + 0.12); // C#5
        
        gain.gain.setValueAtTime(0.2, now);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.5);
        
        osc1.connect(gain);
        osc2.connect(gain);
        gain.connect(audioCtx.destination);
        
        osc1.start(now);
        osc2.start(now);
        osc1.stop(now + 0.5);
        osc2.stop(now + 0.5);
    } catch (e) {
        console.error("Failed to play notification audio:", e);
    }
}

// Start Preview SSE (no alert zone, just bus positions)
function startPreview(patternId) {
    stopPreview();
    
    const url = `/api/monitor?pattern_id=${encodeURIComponent(patternId)}`;
    previewEventSource = new EventSource(url);
    
    previewEventSource.onmessage = (event) => {
        if (!isMonitoring) {
            const payload = JSON.parse(event.data);
            handleMonitoringData(payload);
        }
    };
    
    previewEventSource.onerror = (err) => {
        console.error("Preview SSE connection error:", err);
    };
}

function stopPreview() {
    if (previewEventSource) {
        previewEventSource.close();
        previewEventSource = null;
    }
}

// Start Monitoring via SSE
function startMonitoring() {
    if (isMonitoring) return;
    
    isMonitoring = true;
    alertedVehicles.clear();
    
    // Close the preview stream — we'll replace it with a full monitoring stream
    stopPreview();
    
    // UI state updates
    document.getElementById("startMonitorBtn").style.display = "none";
    document.getElementById("stopMonitorBtn").style.display = "flex";
    document.getElementById("lineInput").disabled = true;
    document.querySelectorAll(".direction-card").forEach(c => c.style.pointerEvents = "none");
    document.querySelectorAll(".action-btn").forEach(b => b.disabled = true);
    
    // Play test sound
    playChime();
    
    // Open EventSource with alert zone parameters
    const url = `/api/monitor?pattern_id=${encodeURIComponent(selectedPattern.id)}&start_stop_id=${encodeURIComponent(startStopId)}&end_stop_id=${encodeURIComponent(endStopId)}`;
    eventSource = new EventSource(url);
    
    eventSource.onmessage = (event) => {
        const payload = JSON.parse(event.data);
        handleMonitoringData(payload);
    };
    
    eventSource.onerror = (err) => {
        console.error("SSE connection error:", err);
        document.getElementById("activeBusesCount").textContent = "Reconnecting stream...";
    };
}

// Stop Monitoring
function stopMonitoring() {
    if (!isMonitoring) return;
    stopPreview();
    
    isMonitoring = false;
    if (eventSource) {
        eventSource.close();
        eventSource = null;
    }
    
    // UI updates
    document.getElementById("startMonitorBtn").style.display = "flex";
    document.getElementById("stopMonitorBtn").style.display = "none";
    document.getElementById("lineInput").disabled = false;
    document.querySelectorAll(".direction-card").forEach(c => c.style.pointerEvents = "auto");
    document.querySelectorAll(".action-btn").forEach(b => b.disabled = false);
    
    // Clear vehicle elements on timeline and map
    document.querySelectorAll(".bus-marker, .bus-tooltip").forEach(el => el.remove());
    clearMapBuses();
    document.getElementById("activeBusesCount").textContent = "0 active buses";
    document.getElementById("activeAlertsContainer").innerHTML = "";
    
    alertedVehicles.clear();
}

async function refreshSegmentDurations() {
    if (!selectedPattern) return;
    try {
        const res = await fetch(`/api/patterns/${selectedPattern.id}/durations`);
        if (!res.ok) return;
        segmentDurations = await res.json();
        document.querySelectorAll('.timeline-segment-info[data-seg-key]').forEach(el => {
            const seg = segmentDurations[el.dataset.segKey];
            const distSpan = el.querySelector('.seg-dist');
            const distHtml = distSpan ? distSpan.outerHTML : '';
            el.innerHTML = distHtml + (seg && seg.sample_count > 0 ? `
                <span class="seg-avg">⌀ ${formatDuration(seg.avg_seconds)} /</span>
                <span class="seg-range">${formatDuration(seg.min_seconds)} ${formatDuration(seg.max_seconds)} · ${seg.sample_count}</span>
            ` : '');
        });
        if (currentView === 'stats') fetchAndRenderHeatmap();
    } catch (e) { /* non-critical */ }
}

function formatEta(seconds) {
    if (seconds == null) return null;
    const mins = Math.round(seconds / 60);
    if (mins <= 0) return "now";
    return `~${mins} min`;
}

function etaArrivalTime(seconds) {
    if (seconds == null) return null;
    const t = new Date(Date.now() + seconds * 1000);
    return t.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function formatDuration(seconds) {
    if (seconds == null) return "?";
    const mins = Math.round(seconds / 60);
    return mins < 1 ? "<1m" : `${mins}m`;
}

// Handle Real-Time Updates
function handleMonitoringData(data) {
    // 1. Update buses count
    const buses = data.buses || [];
    document.getElementById("activeBusesCount").textContent = `${buses.length} active buses`;
    
    // 2. Render buses on timeline
    // First remove old markers
    document.querySelectorAll(".bus-marker, .bus-tooltip").forEach(el => el.remove());
    
    const activeAlerts = [];
    const currentAlertedIds = new Set();

    // Track how many buses land at the same vertical position so we can offset duplicates
    const timelinePosCount = {};

    buses.forEach(bus => {
        const stopNode = document.querySelector(`.timeline-node[data-stop-id="${bus.current_stop_id}"]`);

        if (stopNode) {
            const idx = parseInt(stopNode.dataset.index);
            const stopTop = stopNode.offsetTop;

            let topPosition = stopTop + 2; // Center aligns with the node circle
            let displayStatus = bus.current_status.replace(/_/g, " ").toLowerCase();
            displayStatus = displayStatus.charAt(0).toUpperCase() + displayStatus.slice(1);

            // If the bus is moving (in transit or incoming) and not at the first stop, place it halfway between stops
            if ((bus.current_status === "IN_TRANSIT_TO" || bus.current_status === "INCOMING_AT") && idx > 0) {
                const prevNode = document.querySelector(`.timeline-node[data-index="${idx - 1}"]`);
                if (prevNode) {
                    const prevTop = prevNode.offsetTop;
                    topPosition = (prevTop + stopTop) / 2 + 2;
                    
                    const prevName = prevNode.querySelector(".stop-name").textContent;
                    if (bus.current_status === "INCOMING_AT") {
                        displayStatus = `Arriving at ${bus.current_stop_name}`;
                    } else {
                        displayStatus = `In transit (between ${prevName} and ${bus.current_stop_name})`;
                    }
                }
            } else if (bus.current_status === "STOPPED_AT") {
                displayStatus = `Stopped at ${bus.current_stop_name}`;
            }

            // Offset buses that share the same vertical position
            const posKey = Math.round(topPosition);
            const stackIdx = timelinePosCount[posKey] || 0;
            timelinePosCount[posKey] = stackIdx + 1;
            topPosition += stackIdx * 24;

            // Render bus marker
            const marker = document.createElement("div");
            marker.className = "bus-marker";
            if (bus.is_in_alert_zone) {
                marker.classList.add("alerting");
            }

            // Set absolute layout
            marker.style.position = "absolute";
            marker.style.left = "0px";
            marker.style.top = `${topPosition}px`;
            
            marker.innerHTML = `
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
                    <rect x="2" y="4" width="20" height="15" rx="2" />
                    <line x1="2" y1="14" x2="22" y2="14" />
                    <path d="M6 18h0" stroke-linecap="round" />
                    <path d="M18 18h0" stroke-linecap="round" />
                </svg>
                <span class="bus-plate">${bus.license_plate}</span>
            `;
            
            const tooltip = document.createElement("div");
            tooltip.className = "bus-tooltip";
            tooltip.style.position = "absolute";
            tooltip.style.left = "30px";
            tooltip.style.top = `${topPosition - 4}px`;
            
            const isExpanded = expandedBuses.has(bus.vehicle_id);
            
            const etaStart = formatEta(bus.eta_to_start_seconds);
            const etaStartTime = etaArrivalTime(bus.eta_to_start_seconds);
            const etaEnd = formatEta(bus.eta_to_end_seconds);
            const etaEndTime = etaArrivalTime(bus.eta_to_end_seconds);
            tooltip.innerHTML = `
                <div class="bus-details${isExpanded ? ' open' : ''}">
                    <span class="bus-detail">${(bus.speed !== undefined && bus.speed !== null) ? Number(bus.speed).toFixed(1) : "0"} km/h • ${displayStatus}</span>
                    ${etaStart ? `<span class="bus-detail" style="color:#f59e0b;font-weight:600">Your stop: ${etaStart} · ${etaStartTime}</span>` : ""}
                    ${etaEnd ? `<span class="bus-detail" style="color:#10b981;font-weight:600">Destination: ${etaEnd} · ${etaEndTime}</span>` : ""}
                    ${bus.make ? `<span class="bus-detail">${bus.make} - ${bus.owner} - ${bus.propulsion}</span>` : ""}
                    <span class="bus-detail">ID: ${bus.vehicle_id}  •  Capacity: ${bus.capacity_total}  •  Contactless: ${bus.contactless}  •  Wheelchair: ${bus.wheelchair_accessible}  •  Bikes: ${bus.bikes_allowed}</span>
                </div>
            `;
            
            // Click marker to toggle details
            const toggleExpand = (e) => {
                e.stopPropagation();
                const details = tooltip.querySelector('.bus-details');
                if (details.classList.contains('open')) {
                    details.classList.remove('open');
                    expandedBuses.delete(bus.vehicle_id);
                } else {
                    details.classList.add('open');
                    expandedBuses.add(bus.vehicle_id);
                }
            };
            marker.addEventListener('click', toggleExpand);
            
            const timelineContainer = document.getElementById("routeTimeline");
            timelineContainer.appendChild(marker);
            timelineContainer.appendChild(tooltip);
        }
        
        // 3. Process Alerts
        if (bus.is_in_alert_zone) {
            currentAlertedIds.add(bus.vehicle_id);
            activeAlerts.push(bus);
            
            // If this bus is newly alerted
            if (!alertedVehicles.has(bus.vehicle_id)) {
                alertedVehicles.add(bus.vehicle_id);
                triggerNotification(bus);
            }
        }
    });
    
    // Remove vehicles that are no longer in the alert zone from the alerted set
    for (let id of alertedVehicles) {
        if (!currentAlertedIds.has(id)) {
            // But only remove if it's either gone from the pattern completely OR has left the alert zone
            const stillActiveAndInZone = buses.some(b => b.vehicle_id === id && b.is_in_alert_zone);
            if (!stillActiveAndInZone) {
                alertedVehicles.delete(id);
            }
        }
    }
    
    // 3b. Update map bus markers if in map view
    if (currentView === 'map' && map) {
        updateMapBuses(buses);
    }

    // 4. Update the Active Alerts Banner Panel
    const alertsContainer = document.getElementById("activeAlertsContainer");
    alertsContainer.innerHTML = "";
    
    if (activeAlerts.length > 0) {
        activeAlerts.forEach(bus => {
            const banner = document.createElement("div");
            banner.className = "alert-banner";
            
            const stopsLeft = bus.stops_to_destination;
            let stopsText = stopsLeft === 0 ? "arriving at your stop!" : `approaching! ${stopsLeft} stop${stopsLeft > 1 ? "s" : ""} left.`;
            const etaDestText = formatEta(bus.eta_to_end_seconds);

            banner.innerHTML = `
                <div class="alert-icon">
                    <svg viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5" fill="none">
                        <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
                        <line x1="12" y1="9" x2="12" y2="13"/>
                        <line x1="12" y1="17" x2="12.01" y2="17"/>
                    </svg>
                </div>
                <div class="alert-message">
                    <span class="headline">Bus approaching! (${bus.license_plate})</span>
                    <span class="description">Currently at <strong>${bus.current_stop_name}</strong> (${bus.current_status}) • ${stopsText}${etaDestText ? ` Destination in <strong>${etaDestText}</strong>.` : ""}</span>
                </div>
            `;
            alertsContainer.appendChild(banner);
        });
    } else {
        // If monitoring is running but no bus is in the zone
        const statusBanner = document.createElement("div");
        statusBanner.className = "alert-banner";
        statusBanner.style.background = "rgba(59, 130, 246, 0.05)";
        statusBanner.style.borderColor = "rgba(59, 130, 246, 0.15)";
        statusBanner.style.animation = "none";
        
        statusBanner.innerHTML = `
            <div class="alert-icon" style="background: rgba(59, 130, 246, 0.1); color: #60a5fa;">
                <svg viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" fill="none">
                    <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
                </svg>
            </div>
            <div class="alert-message">
                <span class="headline" style="color: #93c5fd;">Active Monitor Pinging...</span>
                <span class="description" style="color: var(--text-secondary)">Waiting for a bus to enter your alert zone between <strong>${startStopName}</strong> and <strong>${endStopName}</strong>.</span>
            </div>
        `;
        alertsContainer.appendChild(statusBanner);
    }
}

// ---- View switching ----
function setView(view) {
    currentView = view;
    const timelineContainer = document.querySelector('.route-timeline-container');
    const mapView = document.getElementById('mapView');
    const statsView = document.getElementById('statsView');

    timelineContainer.style.display = view === 'timeline' ? '' : 'none';
    mapView.style.display        = view === 'map'      ? 'flex'  : 'none';
    statsView.style.display      = view === 'stats'    ? 'block' : 'none';

    document.getElementById('timelineViewBtn').classList.toggle('active', view === 'timeline');
    document.getElementById('mapViewBtn').classList.toggle('active', view === 'map');
    document.getElementById('statsViewBtn').classList.toggle('active', view === 'stats');

    if (view === 'map') setTimeout(() => initMap(), 0);
    if (view === 'stats') fetchAndRenderHeatmap();
}

async function fetchAndRenderHeatmap() {
    if (!selectedPattern) return;
    const pythonDay = (new Date().getDay() + 6) % 7; // JS Sun=0 → Python Mon=0
    try {
        const res = await fetch(`/api/patterns/${selectedPattern.id}/heatmap?day=${pythonDay}`);
        if (res.ok) {
            heatmapData = await res.json();
            renderHeatmap();
        }
    } catch (e) { /* non-critical */ }
}

function segmentColor(normalized) {
    // Green (#10b981) = fast, Amber (#f59e0b) = slow
    const r = Math.round(16  + normalized * (245 - 16));
    const g = Math.round(185 + normalized * (158 - 185));
    const b = Math.round(129 + normalized * (11  - 129));
    return `rgba(${r},${g},${b},0.4)`;
}

function renderHeatmap() {
    const container = document.getElementById('heatmapContainer');
    const currentHour = new Date().getHours();
    const pythonDay = (new Date().getDay() + 6) % 7;
    const dayName = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"][pythonDay];
    const hours = Array.from({length: 24}, (_, i) => i);

    let html = `<div class="heatmap-title">Travel time per segment — ${dayName}</div>`;
    html += `<div class="heatmap-scroll"><table class="heatmap-table"><thead><tr>`;
    html += `<th class="heatmap-stop-col">Stop</th>`;
    hours.forEach(h => {
        html += `<th class="heatmap-hour-col${h === currentHour ? ' heatmap-current-hour' : ''}">${String(h).padStart(2,'0')}</th>`;
    });
    html += `</tr></thead><tbody>`;

    patternPath.forEach((step, idx) => {
        if (idx === 0) return;
        const prev = patternPath[idx - 1];
        const key = `${prev.stop_id}__${step.stop_id}`;
        const rowData = heatmapData[key] || {};

        const values = hours.map(h => rowData[h]?.avg_seconds).filter(Boolean);
        const rowMin = values.length ? Math.min(...values) : 0;
        const rowMax = values.length ? Math.max(...values) : 0;

        const distM = Math.round((step.distance - prev.distance) * 1000);
        const distLabel = distM > 0 ? ` (${distM >= 1000 ? (distM/1000).toFixed(1)+'km' : distM+'m'})` : '';
        html += `<tr><td class="heatmap-stop-label">${step.stop_sequence}.${distLabel} ${step.name}</td>`;
        hours.forEach(h => {
            const d = rowData[h];
            if (d) {
                const norm = rowMax > rowMin ? (d.avg_seconds - rowMin) / (rowMax - rowMin) : 0.5;
                html += `<td class="heatmap-cell" style="background:${segmentColor(norm)}" title="${formatDuration(d.avg_seconds)} · ${d.sample_count} samples">${formatDuration(d.avg_seconds)}</td>`;
            } else {
                html += `<td class="heatmap-cell heatmap-empty"></td>`;
            }
        });
        html += `</tr>`;
    });

    html += `</tbody></table></div>`;
    container.innerHTML = html;
}

// ---- Map View ----

function initMap() {
    if (!map) {
        // Default center: Lisbon metropolitan area
        map = L.map('leafletMap', { zoomControl: true }).setView([38.72, -9.14], 11);
        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors © <a href="https://carto.com/">CARTO</a>',
            subdomains: 'abcd',
            maxZoom: 19
        }).addTo(map);
        drawRouteOnMap();
    } else {
        map.invalidateSize();
        if (routePolyline) {
            map.fitBounds(routePolyline.getBounds(), { padding: [30, 30] });
        }
    }
}

function drawRouteOnMap() {
    if (!map) return;
    clearMapRoute();

    const coords = patternPath
        .filter(step => step.lat != null && step.lon != null)
        .map(step => [step.lat, step.lon]);

    const lineColor = (selectedLine && selectedLine.color) ? selectedLine.color : '#4f46e5';

    if (patternShape && patternShape.geojson && patternShape.geojson.geometry) {
        // Use precise shape geometry — GeoJSON coords are [lon, lat], Leaflet needs [lat, lon]
        const latlngs = patternShape.geojson.geometry.coordinates.map(c => [c[1], c[0]]);
        routePolyline = L.polyline(latlngs, { color: lineColor, weight: 4, opacity: 0.85 }).addTo(map);
        map.fitBounds(routePolyline.getBounds(), { padding: [30, 30] });
    } else if (coords.length >= 2) {
        // Fallback: straight lines between stops
        routePolyline = L.polyline(coords, { color: lineColor, weight: 4, opacity: 0.85 }).addTo(map);
        map.fitBounds(routePolyline.getBounds(), { padding: [30, 30] });
    }

    patternPath.forEach((step, idx) => {
        if (step.lat == null || step.lon == null) return;
        const isTerminus = idx === 0 || idx === patternPath.length - 1;
        const marker = L.circleMarker([step.lat, step.lon], {
            radius: isTerminus ? 7 : 4,
            fillColor: isTerminus ? lineColor : '#94a3b8',
            color: '#0f172a',
            weight: 1.5,
            opacity: 1,
            fillOpacity: 1
        }).addTo(map);
        marker.bindTooltip(`${step.stop_sequence}. ${step.name}`, { direction: 'top', opacity: 0.95 });
        routeStopMarkers[String(step.stop_id)] = marker;
    });

    refreshMapStopColors();
}

function refreshMapStopColors() {
    if (!map) return;
    const lineColor = (selectedLine && selectedLine.color) ? selectedLine.color : '#4f46e5';
    const lastIdx = patternPath.length - 1;

    patternPath.forEach((step, idx) => {
        const marker = routeStopMarkers[String(step.stop_id)];
        if (!marker) return;
        const isTerminus = idx === 0 || idx === lastIdx;
        const isAlertStart = String(step.stop_id) === String(startStopId);
        const isDestination = String(step.stop_id) === String(endStopId);

        let fillColor, radius;
        if (isAlertStart) {
            fillColor = '#f59e0b';
            radius = 8;
        } else if (isDestination) {
            fillColor = '#10b981';
            radius = 8;
        } else if (isTerminus) {
            fillColor = lineColor;
            radius = 7;
        } else {
            fillColor = '#94a3b8';
            radius = 4;
        }

        marker.setStyle({ fillColor });
        marker.setRadius(radius);
    });
}

function updateMapBuses(buses) {
    if (!map) return;

    const currentIds = new Set(buses.map(b => b.vehicle_id));

    // Remove markers for buses no longer in the feed
    for (const id of Object.keys(busMapMarkers)) {
        if (!currentIds.has(id)) {
            busMapMarkers[id].remove();
            delete busMapMarkers[id];
        }
    }

    // Precompute position groups so co-located buses can be offset
    const posGroups = {};
    buses.forEach(b => {
        if (b.lat == null || b.lon == null) return;
        const key = `${b.lat.toFixed(4)},${b.lon.toFixed(4)}`;
        if (!posGroups[key]) posGroups[key] = [];
        posGroups[key].push(b.vehicle_id);
    });

    buses.forEach(bus => {
        if (bus.lat == null || bus.lon == null) return;

        const posKey = `${bus.lat.toFixed(4)},${bus.lon.toFixed(4)}`;
        const group = posGroups[posKey];
        const stackIdx = group.indexOf(bus.vehicle_id);
        // Spread co-located buses ~25 m apart diagonally (0.00022° ≈ 24 m)
        const lat = bus.lat + stackIdx * 0.00022;
        const lon = bus.lon + stackIdx * 0.00022;

        const alerting = bus.is_in_alert_zone;
        const icon = L.divIcon({
            className: '',
            html: `<div class="map-bus-marker${alerting ? ' alerting' : ''}">${bus.license_plate}</div>`,
            iconSize: [80, 24],
            iconAnchor: [40, 12]
        });

        if (busMapMarkers[bus.vehicle_id]) {
            busMapMarkers[bus.vehicle_id].setLatLng([lat, lon]);
            busMapMarkers[bus.vehicle_id].setIcon(icon);
        } else {
            const marker = L.marker([lat, lon], { icon }).addTo(map);
            marker.bindPopup(`<b>${bus.license_plate}</b><br>${bus.current_stop_name}`);
            busMapMarkers[bus.vehicle_id] = marker;
        }
    });
}

function clearMapBuses() {
    for (const id of Object.keys(busMapMarkers)) {
        busMapMarkers[id].remove();
    }
    busMapMarkers = {};
}

function clearMapRoute() {
    if (routePolyline) {
        routePolyline.remove();
        routePolyline = null;
    }
    Object.values(routeStopMarkers).forEach(m => m.remove());
    routeStopMarkers = {};
}

// Trigger Notifications
function triggerNotification(bus) {
    // Play audio chime
    playChime();
    
    // Browser notification
    const enableNotifications = document.getElementById("notificationToggle").checked;
    if (enableNotifications && Notification.permission === "granted") {
        const title = `Bus Approaching!`;
        const stopsLeft = bus.stops_to_destination;
        const body = `Bus ${bus.license_plate} is at ${bus.current_stop_name}. ${stopsLeft === 0 ? "Arriving now!" : `${stopsLeft} stops away from your destination.`}`;
        
        try {
            new Notification(title, {
                body: body,
                icon: "/static/favicon.ico" // Optional fallback, browser will ignore if missing
            });
        } catch (e) {
            console.error("Failed to display notification:", e);
        }
    }
}
