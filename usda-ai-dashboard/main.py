"""
USDA Agricultural AI Data Dashboard
Aggregates data from USDA NASS, AMS, FNS/SNAP, and NOAA APIs
with built-in anomaly detection for fraud/improper payment analysis.
"""
import os
import json
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from database import init_db, get_db
from data_fetchers import (
    fetch_nass_crop_data,
    fetch_ams_market_prices,
    fetch_snap_data,
    fetch_weather_data,
    run_anomaly_detection,
)
from config import PORT, HOST


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database and seed data on startup."""
    print("Initializing database...")
    init_db()
    print("Fetching initial data from all tiers...")
    try:
        await asyncio.gather(
            fetch_nass_crop_data(),
            fetch_ams_market_prices(),
            fetch_snap_data(),
            fetch_weather_data(),
        )
        print("Running anomaly detection...")
        run_anomaly_detection()
        print("Startup complete — all data loaded.")
    except Exception as e:
        print(f"Warning: Some data fetches failed during startup: {e}")
    yield


app = FastAPI(
    title="USDA Agricultural AI Data Dashboard",
    description="Real-time agricultural market data, SNAP analytics, and AI-powered anomaly detection",
    version="1.0.0",
    lifespan=lifespan,
)

# Serve static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


# ---------------------------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the dashboard."""
    index_path = os.path.join(static_dir, "index.html")
    with open(index_path, "r") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/overview")
async def get_overview():
    """Dashboard overview with key metrics."""
    conn = get_db()
    try:
        commodity_count = conn.execute("SELECT COUNT(DISTINCT commodity) FROM commodity_prices").fetchone()[0]
        latest_prices = conn.execute("""
            SELECT commodity, price, unit, report_date
            FROM commodity_prices
            WHERE price IS NOT NULL
            GROUP BY commodity
            HAVING report_date = MAX(report_date)
            ORDER BY commodity
        """).fetchall()

        snap_latest = conn.execute("""
            SELECT SUM(participants) as total_participants,
                   SUM(benefits_millions) as total_benefits,
                   AVG(avg_benefit_per_person) as avg_benefit
            FROM snap_data
            WHERE year = (SELECT MAX(year) FROM snap_data)
              AND month = (SELECT MAX(month) FROM snap_data WHERE year = (SELECT MAX(year) FROM snap_data))
        """).fetchone()

        anomaly_count = conn.execute("SELECT COUNT(*) FROM anomaly_scores").fetchone()[0]
        high_anomalies = conn.execute("SELECT COUNT(*) FROM anomaly_scores WHERE anomaly_score > 2.0").fetchone()[0]

        api_health = conn.execute("""
            SELECT endpoint, status_code, records_fetched, timestamp
            FROM api_log ORDER BY timestamp DESC LIMIT 10
        """).fetchall()

        return {
            "commodities_tracked": commodity_count,
            "latest_prices": [dict(r) for r in latest_prices],
            "snap_summary": dict(snap_latest) if snap_latest else {},
            "total_anomalies": anomaly_count,
            "high_risk_anomalies": high_anomalies,
            "api_health": [dict(r) for r in api_health],
            "last_updated": datetime.now().isoformat(),
        }
    finally:
        conn.close()


@app.get("/api/commodities")
async def get_commodities():
    """All commodity price data with time series."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT commodity, price, unit, market, report_date
            FROM commodity_prices
            WHERE price IS NOT NULL
            ORDER BY commodity, report_date DESC
        """).fetchall()
        return {"data": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/commodities/{commodity}")
async def get_commodity_detail(commodity: str):
    """Detailed data for a specific commodity."""
    conn = get_db()
    try:
        prices = conn.execute("""
            SELECT price, report_date FROM commodity_prices
            WHERE commodity LIKE ? AND price IS NOT NULL
            ORDER BY report_date
        """, (f"%{commodity}%",)).fetchall()

        crop_stats = conn.execute("""
            SELECT state, year, value, unit FROM crop_statistics
            WHERE commodity LIKE ?
            ORDER BY year DESC, state
        """, (f"%{commodity}%",)).fetchall()

        return {
            "commodity": commodity,
            "price_history": [dict(r) for r in prices],
            "crop_statistics": [dict(r) for r in crop_stats],
        }
    finally:
        conn.close()


@app.get("/api/snap")
async def get_snap_data():
    """SNAP participation and benefit data."""
    conn = get_db()
    try:
        by_state = conn.execute("""
            SELECT state, year, month, participants, benefits_millions, avg_benefit_per_person
            FROM snap_data ORDER BY state, year, month
        """).fetchall()

        national = conn.execute("""
            SELECT year, month,
                   SUM(participants) as total_participants,
                   SUM(benefits_millions) as total_benefits,
                   AVG(avg_benefit_per_person) as avg_benefit
            FROM snap_data
            GROUP BY year, month
            ORDER BY year, month
        """).fetchall()

        return {
            "by_state": [dict(r) for r in by_state],
            "national_trend": [dict(r) for r in national],
        }
    finally:
        conn.close()


@app.get("/api/weather")
async def get_weather_data():
    """Weather data for agricultural regions."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT state, date, temp_avg, precip, drought_index
            FROM weather_data ORDER BY state, date DESC
        """).fetchall()
        return {"data": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/anomalies")
async def get_anomalies():
    """All detected anomalies with risk scoring."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT data_source, record_id, anomaly_score, anomaly_type, description, detected_at
            FROM anomaly_scores
            ORDER BY anomaly_score DESC
        """).fetchall()

        summary = conn.execute("""
            SELECT data_source, anomaly_type, COUNT(*) as count, AVG(anomaly_score) as avg_score
            FROM anomaly_scores
            GROUP BY data_source, anomaly_type
        """).fetchall()

        return {
            "anomalies": [dict(r) for r in rows],
            "summary": [dict(r) for r in summary],
            "total": len(rows),
            "high_risk": len([r for r in rows if r["anomaly_score"] > 2.0]),
        }
    finally:
        conn.close()


@app.get("/api/crop-stats")
async def get_crop_stats():
    """Crop production statistics from NASS."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT commodity, state, year, stat_category, value, unit
            FROM crop_statistics ORDER BY commodity, year DESC, state
        """).fetchall()
        return {"data": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.post("/api/refresh")
async def refresh_data():
    """Manually trigger a data refresh from all sources."""
    try:
        results = await asyncio.gather(
            fetch_nass_crop_data(),
            fetch_ams_market_prices(),
            fetch_snap_data(),
            fetch_weather_data(),
            return_exceptions=True,
        )
        anomalies = run_anomaly_detection()
        return {
            "status": "success",
            "nass_records": len(results[0]) if not isinstance(results[0], Exception) else f"error: {results[0]}",
            "ams_records": len(results[1]) if not isinstance(results[1], Exception) else f"error: {results[1]}",
            "snap_records": len(results[2]) if not isinstance(results[2], Exception) else f"error: {results[2]}",
            "weather_records": len(results[3]) if not isinstance(results[3], Exception) else f"error: {results[3]}",
            "anomalies_detected": len(anomalies),
            "refreshed_at": datetime.now().isoformat(),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


# ---------------------------------------------------------------------------
# RUN
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=HOST, port=PORT, reload=True)
