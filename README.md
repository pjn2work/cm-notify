# Carris Metropolitana Bus Notifier

A real-time bus tracking and notification application for **Carris Metropolitana** (Lisbon metropolitan area public transport). The backend is built with **FastAPI** (Python) and uses **Server-Sent Events (SSE)** to stream live vehicle coordinates and arrival proximity alerts to a single-page web frontend.

---

## Features

- **Real-Time Tracking**: Queries vehicle locations every 10 seconds in a background thread and streams updates to clients.
- **Server-Sent Events (SSE)**: Streams real-time bus positions, speed, bearing, and distance to stops using a persistent HTTP connection.
- **Proximity Alerts**: Allows users to specify a start and destination stop, highlighting when a bus enters the monitored "alert zone" and counting remaining stops.
- **Caching Layer**: Caches static data (lines, stops, patterns) locally from the Carris Metropolitana API to keep response times fast and reduce load on external APIs.

---

## Prerequisites

- **Python 3.10 or higher**
- A virtual environment tool (`venv` or `poetry`)

---

## Installation & Setup

1. **Activate the Virtual Environment**:
   If you are using the pre-configured virtual environment in `.venv/`:
   ```bash
   source .venv/bin/activate
   ```
   *(Or use the local alias if configured on your shell).*

2. **Install Dependencies**:
   Install the required libraries listed in `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   ```

---

## How to Run the Application

You can run the FastAPI server using **Uvicorn**, a lightning-fast ASGI server implementation. It is recommended to invoke it via the active virtual environment's Python interpreter (`python -m uvicorn`) to avoid any virtual environment shebang path conflicts:

### 1. Development Mode (with Auto-Reload)
To run the server with auto-reload enabled (so the server restarts automatically when you make code changes):
```bash
python -m uvicorn main:app --reload
```

### 2. Production Mode (Standard run)
To run the server without auto-reload:
```bash
python -m uvicorn main:app
```

### 3. Custom Port and Host
By default, the server runs on `http://127.0.0.1:8000`. You can change this using the `--host` and `--port` flags:
```bash
python -m uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

---

## Project Structure

```text
├── main.py             # FastAPI backend with background polling & API endpoints
├── requirements.txt    # Python library dependencies
├── static/             # Single-Page Application (SPA) frontend
│   ├── index.html      # Main HTML structure
│   ├── app.js          # Live tracking map & SSE client logic
│   └── styles.css      # Styling for the dashboard
└── README.md           # This documentation file
```

---

## Backend API Endpoints

### Static & UI Endpoints
- `GET /` - Serves the single-page application (`static/index.html`).
- `GET /static/...` - Serves frontend assets.

### Data & Monitoring Endpoints
- `GET /api/lines?search=<query>` - Retrieve list of lines, with optional text search/filtering.
- `GET /api/lines/{line_id}/patterns` - Retrieve all patterns/directions for a specific line.
- `GET /api/patterns/{pattern_id}` - Retrieve details for a pattern path, including stop sequence and coordinate information.
- `GET /api/monitor?pattern_id=<id>&start_stop_id=<stop_id>&end_stop_id=<stop_id>` - Establish a Server-Sent Events connection streaming real-time bus locations and calculations (such as stops left to target, active bus count) for a specified pattern.
