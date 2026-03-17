"""
USDA Agricultural AI Data Dashboard
Aggregates data from USDA NASS, AMS, FNS/SNAP, and NOAA APIs
with multi-signal anomaly detection for SNAP benefits and commodity markets.
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
    print("Initializing database...", flush=True)
    try:
        init_db()
        print("Database initialized.", flush=True)
    except Exception as e:
        print(f"Database init error: {e}", flush=True)

    # Load real data from CSVs (instant) + seed data for market/weather/crop
    try:
        from data_fetchers import (
            load_real_snap_data, seed_crop_data, seed_market_data, seed_weather_data,
            save_crop_data, save_commodity_prices, save_snap_data, save_weather_data,
        )
        # SNAP: real FNS-sourced data from CSV
        save_snap_data(load_real_snap_data())
        # Market/Crop/Weather: seed data (live APIs attempted in background)
        save_commodity_prices(seed_market_data())
        save_crop_data(seed_crop_data())
        save_weather_data(seed_weather_data())
        # Multi-signal anomaly detection across all data
        run_anomaly_detection()
        print("Real SNAP data + seed data loaded, multi-signal anomaly detection complete.", flush=True)
    except Exception as e:
        print(f"Data load error: {e}", flush=True)

    # Then try live API data in background (won't block startup)
    async def refresh_from_apis():
        try:
            print("Fetching live data from APIs...", flush=True)
            results = await asyncio.gather(
                fetch_nass_crop_data(),
                fetch_ams_market_prices(),
                fetch_snap_data(),
                fetch_weather_data(),
                return_exceptions=True,
            )
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"  Tier fetch {i} failed: {r}", flush=True)
            run_anomaly_detection()
            print("Live data refresh complete.", flush=True)
        except Exception as e:
            print(f"Warning: Live data fetch failed (seed data still available): {e}", flush=True)

    asyncio.create_task(refresh_from_apis())
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
        latest_prices_raw = conn.execute("""
            SELECT commodity, price, unit, report_date
            FROM commodity_prices
            WHERE price IS NOT NULL
            GROUP BY commodity
            HAVING report_date = MAX(report_date)
            ORDER BY commodity
        """).fetchall()

        # Calculate % change from 30-day mean for each commodity (unit-agnostic)
        latest_prices = []
        for row in latest_prices_raw:
            r = dict(row)
            avg_row = conn.execute(
                "SELECT AVG(price) as avg_price FROM commodity_prices WHERE commodity = ? AND price IS NOT NULL",
                (r["commodity"],)
            ).fetchone()
            if avg_row and avg_row["avg_price"] and avg_row["avg_price"] != 0:
                r["pct_change"] = round((r["price"] - avg_row["avg_price"]) / avg_row["avg_price"] * 100, 2)
                r["avg_30d"] = round(avg_row["avg_price"], 2)
            else:
                r["pct_change"] = 0
                r["avg_30d"] = r["price"]
            latest_prices.append(r)

        snap_latest = conn.execute("""
            SELECT SUM(participants) as total_participants,
                   SUM(benefits_millions) as total_benefits,
                   ROUND(SUM(benefits_millions) * 1000000.0 / NULLIF(SUM(participants), 0), 2) as avg_benefit
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
                   ROUND(SUM(benefits_millions) * 1000000.0 / NULLIF(SUM(participants), 0), 2) as avg_benefit
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
            SELECT id, data_source, record_id, anomaly_score, anomaly_type, description, signals, detected_at
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


@app.get("/api/anomalies/{anomaly_id}")
async def get_anomaly_detail(anomaly_id: int):
    """Deep-dive into a single anomaly: how it was calculated, historical context,
    and cross-reference data from corroboration sources."""
    conn = get_db()
    try:
        anomaly = conn.execute(
            "SELECT * FROM anomaly_scores WHERE id = ?", (anomaly_id,)
        ).fetchone()
        if not anomaly:
            return JSONResponse(status_code=404, content={"error": "Anomaly not found"})
        anomaly = dict(anomaly)

        # Parse stored signals JSON
        signals = {}
        if anomaly.get("signals"):
            try:
                signals = json.loads(anomaly["signals"])
            except (json.JSONDecodeError, TypeError):
                pass

        result = {"anomaly": anomaly, "calculation": {}, "history": [], "corroboration": [], "signals": signals}

        if anomaly["data_source"] == "snap_data":
            # Get the flagged record
            record = conn.execute(
                "SELECT * FROM snap_data WHERE id = ?", (anomaly["record_id"],)
            ).fetchone()
            if record:
                record = dict(record)
                result["flagged_record"] = record

                # Get ALL records for this state to show how z-score was calculated
                all_state = conn.execute(
                    "SELECT id, year, month, participants, benefits_millions, avg_benefit_per_person "
                    "FROM snap_data WHERE state = ? ORDER BY year, month",
                    (record["state"],)
                ).fetchall()
                all_state = [dict(r) for r in all_state]
                result["history"] = all_state

                # Recalculate z-score step by step for the explainer
                values = [r["avg_benefit_per_person"] for r in all_state if r["avg_benefit_per_person"]]
                n = len(values)
                mean_val = sum(values) / n
                std_val = (sum((x - mean_val) ** 2 for x in values) / (n - 1)) ** 0.5
                flagged_val = record["avg_benefit_per_person"]
                z = abs(flagged_val - mean_val) / std_val if std_val > 0 else 0

                result["calculation"] = {
                    "metric": "avg_benefit_per_person",
                    "flagged_value": round(flagged_val, 2),
                    "mean": round(mean_val, 2),
                    "std_dev": round(std_val, 2),
                    "n_observations": n,
                    "z_score": round(z, 3),
                    "threshold": 1.8,
                    "formula": f"|${flagged_val:.2f} - ${mean_val:.2f}| / ${std_val:.2f} = {z:.3f}",
                    "explanation": (
                        f"This state's avg benefit of ${flagged_val:.2f} is {z:.1f} standard deviations "
                        f"from the mean of ${mean_val:.2f} across {n} monthly observations. "
                        f"Values above z=1.8 are flagged. "
                        f"{'This is a strong signal — worth investigating.' if z > 3.0 else 'Moderate signal — review recommended.' if z > 2.0 else 'Weak signal — may be normal variation.'}"
                    ),
                    "percentile": round(len([v for v in values if v < flagged_val]) / n * 100, 1),
                }

                # Cross-reference: what was happening with commodity prices that month?
                commodity_context = conn.execute(
                    "SELECT commodity, AVG(price) as avg_price, COUNT(*) as records "
                    "FROM commodity_prices "
                    "WHERE report_date LIKE ? AND price IS NOT NULL "
                    "GROUP BY commodity",
                    (f"{record['year']}-{record['month']:02d}%",)
                ).fetchall()
                if commodity_context:
                    result["corroboration"].append({
                        "source": "AMS Commodity Prices",
                        "description": f"Food commodity prices for {record['year']}-{record['month']:02d}",
                        "data": [dict(r) for r in commodity_context],
                        "relevance": "Higher food prices can trigger SNAP benefit recalculations under COLA adjustments",
                    })

                # Cross-reference: weather anomalies in nearby states
                weather_context = conn.execute(
                    "SELECT state, AVG(temp_avg) as avg_temp, AVG(precip) as avg_precip, "
                    "AVG(drought_index) as avg_drought "
                    "FROM weather_data "
                    "WHERE date LIKE ? "
                    "GROUP BY state",
                    (f"{record['year']}-{record['month']:02d}%",)
                ).fetchall()
                if weather_context:
                    result["corroboration"].append({
                        "source": "NOAA Weather Data",
                        "description": f"Agricultural weather conditions for {record['year']}-{record['month']:02d}",
                        "data": [dict(r) for r in weather_context],
                        "relevance": "Severe weather/drought can cause crop failures, increasing food insecurity and SNAP enrollment",
                    })

                # Other anomalies in the same time period
                nearby_anomalies = conn.execute(
                    "SELECT a.*, s.state, s.year, s.month, s.avg_benefit_per_person "
                    "FROM anomaly_scores a "
                    "JOIN snap_data s ON a.record_id = s.id AND a.data_source = 'snap_data' "
                    "WHERE s.year = ? AND s.month = ? AND a.id != ? "
                    "ORDER BY a.anomaly_score DESC",
                    (record["year"], record["month"], anomaly_id)
                ).fetchall()
                if nearby_anomalies:
                    result["corroboration"].append({
                        "source": "Concurrent SNAP Anomalies",
                        "description": f"Other states flagged in {record['year']}-{record['month']:02d}",
                        "data": [dict(r) for r in nearby_anomalies],
                        "relevance": "Multiple states spiking simultaneously suggests a systemic cause (policy change, COLA); isolated spikes suggest state-level issues",
                    })

        elif anomaly["data_source"] == "commodity_prices":
            record = conn.execute(
                "SELECT * FROM commodity_prices WHERE id = ?", (anomaly["record_id"],)
            ).fetchone()
            if record:
                record = dict(record)
                result["flagged_record"] = record

                all_commodity = conn.execute(
                    "SELECT id, commodity, price, report_date "
                    "FROM commodity_prices WHERE commodity = ? AND price IS NOT NULL "
                    "ORDER BY report_date",
                    (record["commodity"],)
                ).fetchall()
                all_commodity = [dict(r) for r in all_commodity]
                result["history"] = all_commodity

                prices = [r["price"] for r in all_commodity]
                n = len(prices)
                mean_val = sum(prices) / n
                std_val = (sum((x - mean_val) ** 2 for x in prices) / (n - 1)) ** 0.5
                flagged_val = record["price"]
                z = abs(flagged_val - mean_val) / std_val if std_val > 0 else 0

                result["calculation"] = {
                    "metric": "price",
                    "flagged_value": round(flagged_val, 2),
                    "mean": round(mean_val, 2),
                    "std_dev": round(std_val, 2),
                    "n_observations": n,
                    "z_score": round(z, 3),
                    "threshold": 1.5,
                    "formula": f"|${flagged_val:.2f} - ${mean_val:.2f}| / ${std_val:.2f} = {z:.3f}",
                    "explanation": (
                        f"This {record['commodity']} price of ${flagged_val:.2f} is {z:.1f} standard deviations "
                        f"from the 30-day mean of ${mean_val:.2f}. "
                        f"{'Strong outlier — check for data entry error or market event.' if z > 3.0 else 'Notable deviation — review market conditions.' if z > 2.0 else 'Mild outlier — likely normal volatility.'}"
                    ),
                }

        return result
    finally:
        conn.close()


@app.get("/api/corroboration/{state}/{year}/{month}")
async def get_corroboration_data(state: str, year: int, month: int):
    """Fetch external corroboration data for anomaly investigation.
    Pulls from BLS CPI, FEMA disasters, and USDA food price index."""
    import httpx

    results = {"state": state, "period": f"{year}-{month:02d}", "sources": []}

    # 1. BLS Consumer Price Index — Food at Home
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # BLS public API (no key needed for limited access)
            resp = await client.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", json={
                "seriesid": ["CUSR0000SAF11"],  # CPI Food at Home, US city avg
                "startyear": str(year - 1),
                "endyear": str(year),
            })
            if resp.status_code == 200:
                bls_data = resp.json()
                series = bls_data.get("Results", {}).get("series", [{}])[0].get("data", [])
                relevant = [s for s in series if s.get("year") == str(year) and s.get("period") == f"M{month:02d}"]
                if relevant:
                    results["sources"].append({
                        "name": "BLS CPI — Food at Home",
                        "value": relevant[0].get("value"),
                        "period": f"{year}-{month:02d}",
                        "relevance": "Rising food CPI explains higher SNAP benefits (COLA adjustments)",
                        "url": "https://www.bls.gov/cpi/",
                    })
                # Also get prior year for comparison
                prior = [s for s in series if s.get("year") == str(year - 1) and s.get("period") == f"M{month:02d}"]
                if relevant and prior:
                    try:
                        yoy_change = round((float(relevant[0]["value"]) - float(prior[0]["value"])) / float(prior[0]["value"]) * 100, 1)
                        results["sources"].append({
                            "name": "Food CPI Year-over-Year Change",
                            "value": f"{yoy_change}%",
                            "period": f"{year-1} → {year}",
                            "relevance": f"{'Food inflation elevated — supports higher benefit levels' if yoy_change > 3 else 'Food inflation moderate — benefit spike less explainable by CPI alone'}",
                        })
                    except (ValueError, ZeroDivisionError):
                        pass
    except Exception as e:
        results["sources"].append({"name": "BLS CPI", "error": str(e), "status": "unavailable"})

    # 2. FEMA Disaster Declarations
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            fema_url = f"https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
            resp = await client.get(fema_url, params={
                "$filter": f"state eq '{state[:2].upper()}' and fyDeclared eq {year}",
                "$top": 10,
                "$orderby": "declarationDate desc",
            })
            if resp.status_code == 200:
                disasters = resp.json().get("DisasterDeclarationsSummaries", [])
                if disasters:
                    results["sources"].append({
                        "name": "FEMA Disaster Declarations",
                        "count": len(disasters),
                        "declarations": [{"type": d.get("incidentType"), "title": d.get("declarationTitle"),
                                         "date": d.get("declarationDate", "")[:10]} for d in disasters[:5]],
                        "relevance": "Disaster declarations trigger emergency SNAP benefits (D-SNAP), causing benefit spikes",
                        "url": "https://www.fema.gov/disaster/declarations",
                    })
                else:
                    results["sources"].append({
                        "name": "FEMA Disaster Declarations",
                        "count": 0,
                        "relevance": "No disaster declarations found — benefit spike not explained by emergency SNAP",
                    })
    except Exception as e:
        results["sources"].append({"name": "FEMA Disasters", "error": str(e), "status": "unavailable"})

    # 3. USDA ERS Food Price Outlook
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # ERS publishes food price forecasts
            resp = await client.get("https://api.ers.usda.gov/data/food-price-outlook/summary", params={
                "year": year,
            })
            if resp.status_code == 200:
                ers_data = resp.json()
                results["sources"].append({
                    "name": "USDA ERS Food Price Outlook",
                    "data": ers_data,
                    "relevance": "USDA's own food price forecasts — high forecasts support higher SNAP benefits",
                })
    except Exception as e:
        results["sources"].append({"name": "USDA ERS Food Price Outlook", "error": str(e), "status": "unavailable"})

    return results


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
