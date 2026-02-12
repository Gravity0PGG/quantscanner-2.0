from fastapi import FastAPI, Request, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from orchestrator import Orchestrator
import json
import glob
import os
import pandas as pd
from datetime import datetime
import asyncio
from loguru import logger as loguru_logger
from fastapi.responses import StreamingResponse
import queue
import threading
from services.chart_engine import ChartEngine

app = FastAPI(title="QuantScanner 2026 Dashboard")

# Create templates/fragments if it doesn't exist
os.makedirs("templates/fragments", exist_ok=True)
os.makedirs("data/snapshots", exist_ok=True)

# Mounting static files
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Globals
orch = Orchestrator()
chart_engine = ChartEngine()
# State to track active scan
active_scan = {"status": "idle", "last_run": None, "current_candidates": []}
# WebSocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

# WebSocket endpoint
@app.websocket("/ws/progress")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() # Keep alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "status": active_scan["status"],
        "last_run": active_scan["last_run"]
    })

@app.post("/run-scan")
async def run_scan(background_tasks: BackgroundTasks):
    if active_scan["status"] == "running":
        return "<button disabled class='bg-gray-600 text-white font-bold py-2 px-6 rounded-lg opacity-50 cursor-not-allowed'>Scan in Progress...</button>"
    
    active_scan["status"] = "running"
    active_scan["last_run"] = datetime.now().strftime("%H:%M:%S")
    background_tasks.add_task(execute_scan, full_universe=False)
    
    return """
    <button disabled class='bg-orange-600 text-white font-bold py-2 px-6 rounded-lg animate-pulse'>
        <span class='flex items-center'>
            <svg class="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
            System Scanning...
        </span>
    </button>
    """

# Background task for live price updates
async def live_ltp_updater():
    while True:
        if active_scan["current_candidates"]:
            try:
                tickers = active_scan["current_candidates"]
                prices = orch.dm.batch_fetch_live_prices(tickers)
                if prices:
                    await manager.broadcast({"type": "ltp_update", "prices": prices})
            except Exception as e:
                loguru_logger.error(f"LTP Update Error: {e}")
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup_event():
    app.state.loop = asyncio.get_running_loop()
    
    # Trigger weekly cleanup (168 hours)
    chart_engine.cleanup_old_charts(max_age_hours=168)
    
    # Start live LTP updater
    asyncio.create_task(live_ltp_updater())

@app.post("/run-full-scan")
async def run_full_scan(background_tasks: BackgroundTasks):
    """Triggers the discovery-based full market scan."""
    if active_scan["status"] == "running":
        return "<button disabled class='bg-gray-600 text-white font-bold py-2 px-6 rounded-lg opacity-50'>Full Scan Busy...</button>"
    
    active_scan["status"] = "running"
    active_scan["last_run"] = datetime.now().strftime("%H:%M:%S")
    background_tasks.add_task(execute_scan, full_universe=True)
    
    return """
    <button disabled class='bg-purple-600 text-white font-bold py-2 px-6 rounded-lg animate-pulse'>
        Discovery Scanning...
    </button>
    """

async def execute_scan(full_universe: bool = False):
    def progress_callback(msg, pct):
        asyncio.run_coroutine_threadsafe(
            manager.broadcast({"type": "progress", "message": msg, "percent": pct}),
            app.state.loop
        )

    try:
        # Run orchestrated scan
        results = await orch.run_scan(full_universe=full_universe, progress_callback=progress_callback)
        active_scan["current_candidates"] = [r["ticker"] for r in results] if results else []
        
    except Exception as e:
        loguru_logger.error(f"Scan Error: {e}")
        await manager.broadcast({"type": "error", "message": f"Scan failed: {str(e)}"})
    finally:
        active_scan["status"] = "idle"
        await manager.broadcast({"type": "scan_complete"})

# --- View Components (DRY) ---

@app.get("/health", response_class=HTMLResponse)
async def get_health(request: Request):
    report = orch.health.run()
    status_color = "text-green-500" if report.passed else "text-red-500"
    status_text = "SYSTEM READY" if report.passed else "SYSTEM ERROR"
    return f"""
    <div class='flex items-center space-x-2'>
        <span class='w-2 h-2 rounded-full bg-current {status_color} animate-pulse'></span>
        <span class='font-bold {status_color}'>{status_text}</span>
    </div>
    """

@app.get("/springs", response_class=HTMLResponse)
async def get_springs(request: Request):
    audit_files = glob.glob(os.path.join("logs", "audit_*.json"))
    if not audit_files: return "<div class='text-gray-500 p-4'>No data</div>"
    latest_audit = max(audit_files, key=os.path.getmtime)
    with open(latest_audit, 'r') as f: data = json.load(f)
    
    cap_filter = request.query_params.get("cap", "ALL")
    
    # Prefer pulling enriched data from 'candidates' if available
    candidates = data.get("candidates", [])
    springs = []
    
    # 1. Try to find enriched springs in candidates
    for c in candidates:
        if c.get("status") == "COILING_SPRING":
            if cap_filter == "ALL" or c.get("cap") == cap_filter:
                springs.append(c)
    
    # 2. Fallback to rationale for basics if candidates don't have enough
    if not springs:
        rationale = data.get("rationale", {})
        for t, g in rationale.items():
            if g.get("status") == "COILING_SPRING":
                if cap_filter == "ALL" or g.get("cap", "SMALL") == cap_filter:
                    springs.append({
                        "ticker": t,
                        "reason": g.get("reason", "Consolidating"),
                        "mrs": g.get("mrs", 0),
                        "pattern": g.get("pattern")
                    })
    
    return templates.TemplateResponse("fragments/springs.html", {"request": request, "springs": springs})

@app.get("/buy_signals", response_class=HTMLResponse)
async def get_buy_signals(request: Request):
    audit_files = glob.glob(os.path.join("data", "snapshots", "snapshot_*.json"))
    if not audit_files: return "<div class='text-gray-500 p-4'>No signals</div>"
    latest_audit = max(audit_files, key=os.path.getmtime)
    with open(latest_audit, 'r') as f: data = json.load(f)
    
    cap_filter = request.query_params.get("cap", "ALL")
    candidates = data.get("candidates", []) # Mixed list of BUY and COILING_SPRING
    
    signals = []
    for c in candidates:
        # ONLY show 'BUY' status in the primary Signals container
        if c.get("status") == "BUY":
            if cap_filter == "ALL" or c.get("cap") == cap_filter:
                signals.append(c)
    
    return templates.TemplateResponse("fragments/buy_signals.html", {"request": request, "signals": signals})

@app.get("/logs", response_class=HTMLResponse)
async def get_logs(request: Request):
    log_file = "logs/system.log"
    if not os.path.exists(log_file): return "<div>Awaiting logs...</div>"
    with open(log_file, "r") as f:
        lines = f.readlines()[-25:]
    
    formatted_lines = []
    for line in lines:
        if any(x in line for x in ["ERROR", "CRITICAL"]): color = "text-red-400"
        elif "SUCCESS" in line: color = "text-green-400"
        elif "Discovery" in line or "Scanned" in line: color = "text-blue-400"
        else: color = "text-gray-300"
        formatted_lines.append(f"<div class='{color} font-mono text-[10px] py-0.5'>{line.strip()}</div>")
    
    return "".join(formatted_lines)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
