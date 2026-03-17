"""
Data fetchers for all three tiers of USDA agricultural data.
- Tier 1: Real SNAP data from FNS published CSVs + NASS crop stats
- Tier 2: AMS market prices (live API when key available, seed fallback)
- Tier 3: NOAA weather + BLS unemployment + BLS CPI (enrichment)
Multi-signal anomaly detection across 5 correlated signals.
"""
import csv
import httpx
import json
import os
import random
from datetime import datetime, timedelta
from database import get_db, log_api_call
from config import NASS_API_KEY, NOAA_API_TOKEN, NASS_BASE_URL, AMS_BASE_URL, NOAA_BASE_URL

TIMEOUT = 15.0
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


# ---------------------------------------------------------------------------
# REAL DATA LOADERS — CSV files sourced from published USDA/BLS reports
# ---------------------------------------------------------------------------
def load_real_snap_data():
    """Load SNAP data from FNS-sourced CSV (state-level monthly participation & benefits).
    Source: USDA FNS SNAP Data Tables, FY2023-2025
    https://www.fns.usda.gov/pd/supplemental-nutrition-assistance-program-snap
    """
    csv_path = os.path.join(DATA_DIR, "snap_state_monthly.csv")
    records = []
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append({
                    "state": row["state"],
                    "year": int(row["year"]),
                    "month": int(row["month"]),
                    "participants": int(row["participants"]),
                    "benefits_millions": float(row["benefits_millions"]),
                    "avg_benefit_per_person": float(row["avg_benefit_per_person"]),
                })
        print(f"[SNAP] Loaded {len(records)} real records from FNS data", flush=True)
        log_api_call("FNS/SNAP-CSV", 200, len(records))
    except Exception as e:
        print(f"[SNAP] CSV load failed: {e} — falling back to seed data", flush=True)
        records = seed_snap_data()
    return records


def load_qc_error_rates():
    """Load QC payment error rates from FNS published reports.
    Source: USDA FNS SNAP Quality Control Annual Reports, FY2022-2024
    https://www.fns.usda.gov/snap/qc/per
    National FY2024: 10.93% total (9.26% overpayment, 1.67% underpayment)
    """
    csv_path = os.path.join(DATA_DIR, "qc_error_rates.csv")
    records = []
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append({
                    "state": row["state"],
                    "fy": int(row["fy"]),
                    "total_error_rate": float(row["total_error_rate"]),
                    "overpayment_rate": float(row["overpayment_rate"]),
                    "underpayment_rate": float(row["underpayment_rate"]),
                })
        print(f"[QC] Loaded {len(records)} QC error rate records", flush=True)
    except Exception as e:
        print(f"[QC] CSV load failed: {e}", flush=True)
    return records


def load_unemployment_data():
    """Load BLS LAUS unemployment data by state.
    Source: BLS Local Area Unemployment Statistics
    https://www.bls.gov/lau/
    """
    csv_path = os.path.join(DATA_DIR, "unemployment_rates.csv")
    records = []
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append({
                    "state": row["state"],
                    "year": int(row["year"]),
                    "month": int(row["month"]),
                    "unemployment_rate": float(row["unemployment_rate"]),
                })
        print(f"[BLS] Loaded {len(records)} unemployment records", flush=True)
    except Exception as e:
        print(f"[BLS] CSV load failed: {e}", flush=True)
    return records


def load_food_cpi():
    """Load BLS CPI-U Food at Home index.
    Source: BLS CPI Series CUSR0000SAF11
    https://www.bls.gov/cpi/
    """
    csv_path = os.path.join(DATA_DIR, "food_cpi.csv")
    records = []
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append({
                    "year": int(row["year"]),
                    "month": int(row["month"]),
                    "cpi_food_at_home": float(row["cpi_food_at_home"]),
                    "cpi_all_items": float(row["cpi_all_items"]),
                })
        print(f"[CPI] Loaded {len(records)} CPI records", flush=True)
    except Exception as e:
        print(f"[CPI] CSV load failed: {e}", flush=True)
    return records


# ---------------------------------------------------------------------------
# TIER 1 — USDA NASS QuickStats (Crop statistics & farm economics)
# ---------------------------------------------------------------------------
async def fetch_nass_crop_data():
    """Fetch crop production and price data from USDA NASS QuickStats."""
    if not NASS_API_KEY:
        print("[NASS] No API key — using seed data", flush=True)
        log_api_call("NASS/QuickStats", 0, 0, "No API key configured — using seed data")
        return seed_crop_data()

    commodities = ["CORN", "SOYBEANS", "WHEAT", "COTTON", "RICE"]
    all_records = []

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for commodity in commodities:
            try:
                params = {
                    "key": NASS_API_KEY,
                    "commodity_desc": commodity,
                    "statisticcat_desc": "PRICE RECEIVED",
                    "year__GE": "2023",
                    "agg_level_desc": "NATIONAL",
                    "format": "JSON",
                }
                resp = await client.get(f"{NASS_BASE_URL}/api_GET/", params=params)

                if resp.status_code == 200:
                    data = resp.json().get("data", [])
                    for row in data[:20]:
                        try:
                            value = float(row.get("Value", "0").replace(",", ""))
                        except (ValueError, TypeError):
                            continue
                        all_records.append({
                            "commodity": row.get("commodity_desc", commodity),
                            "state": row.get("state_name", "US TOTAL"),
                            "year": int(row.get("year", 2024)),
                            "stat_category": row.get("statisticcat_desc", ""),
                            "value": value,
                            "unit": row.get("unit_desc", ""),
                        })
                    log_api_call(f"NASS/{commodity}", resp.status_code, len(data))
                else:
                    log_api_call(f"NASS/{commodity}", resp.status_code, 0, resp.text[:200])
            except Exception as e:
                log_api_call(f"NASS/{commodity}", 0, 0, str(e)[:200])

    if not all_records:
        return seed_crop_data()

    save_crop_data(all_records)
    return all_records


# ---------------------------------------------------------------------------
# TIER 2 — USDA AMS Market News (Live commodity prices)
# Only attempts API calls when properly configured; logs status cleanly
# ---------------------------------------------------------------------------
AMS_API_KEY = os.getenv("AMS_API_KEY", "")

async def fetch_ams_market_prices():
    """Fetch live market prices from USDA Agricultural Marketing Service.
    Only attempts API calls if AMS_API_KEY is configured to avoid 403 errors.
    """
    if not AMS_API_KEY:
        print("[AMS] No API key — using seed market data", flush=True)
        log_api_call("AMS/MarketNews", 0, 0, "No API key configured — using seed data")
        records = seed_market_data()
        save_commodity_prices(records)
        return records

    all_records = []
    report_slugs = [
        {"slug": "2466", "name": "National Daily Cattle & Beef"},
        {"slug": "2468", "name": "National Daily Hog & Pork"},
        {"slug": "1095", "name": "USDA Grain Transportation Report"},
    ]

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for rpt in report_slugs:
            try:
                url = f"{AMS_BASE_URL}/reports/{rpt['slug']}"
                resp = await client.get(url, headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {AMS_API_KEY}",
                })

                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get("results", data) if isinstance(data, dict) else data
                    if isinstance(results, list):
                        for item in results[:15]:
                            record = parse_ams_record(item, rpt["name"])
                            if record:
                                all_records.append(record)
                    log_api_call(f"AMS/{rpt['slug']}", resp.status_code, len(results) if isinstance(results, list) else 1)
                else:
                    log_api_call(f"AMS/{rpt['slug']}", resp.status_code, 0, resp.text[:200])
            except Exception as e:
                log_api_call(f"AMS/{rpt['slug']}", 0, 0, str(e)[:200])

    if not all_records:
        all_records = seed_market_data()

    save_commodity_prices(all_records)
    return all_records


def parse_ams_record(item, report_name):
    """Parse an AMS market record into our standard format."""
    if not isinstance(item, dict):
        return None
    price = None
    for key in ["avg_price", "price", "wtd_avg", "weighted_average", "low_price"]:
        if key in item:
            try:
                price = float(str(item[key]).replace(",", "").replace("$", ""))
                break
            except (ValueError, TypeError):
                continue
    return {
        "commodity": item.get("commodity", item.get("class", report_name)),
        "price": price,
        "unit": item.get("unit", "$/CWT"),
        "market": item.get("market", item.get("region", "National")),
        "report_date": item.get("report_date", item.get("published_date", datetime.now().strftime("%Y-%m-%d"))),
    }


# ---------------------------------------------------------------------------
# TIER 2 — FNS SNAP Participation Data
# ---------------------------------------------------------------------------
async def fetch_snap_data():
    """Load SNAP data from real FNS-sourced CSV files."""
    records = load_real_snap_data()
    save_snap_data(records)
    return records


# ---------------------------------------------------------------------------
# TIER 3 — NOAA Weather & Climate Data
# ---------------------------------------------------------------------------
async def fetch_weather_data():
    """Fetch weather data for agricultural regions from NOAA."""
    if not NOAA_API_TOKEN:
        print("[NOAA] No API token — using seed data", flush=True)
        log_api_call("NOAA/Weather", 0, 0, "No API key configured — using seed data")
        records = seed_weather_data()
        save_weather_data(records)
        return records

    records = []
    farm_states = [
        {"fips": "FIPS:17", "name": "Illinois"},
        {"fips": "FIPS:19", "name": "Iowa"},
        {"fips": "FIPS:20", "name": "Kansas"},
        {"fips": "FIPS:27", "name": "Minnesota"},
        {"fips": "FIPS:31", "name": "Nebraska"},
    ]

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for state in farm_states:
            try:
                params = {
                    "datasetid": "GHCND",
                    "locationid": state["fips"],
                    "startdate": start_date,
                    "enddate": end_date,
                    "datatypeid": "TAVG,PRCP",
                    "units": "standard",
                    "limit": 25,
                }
                headers = {"token": NOAA_API_TOKEN}
                resp = await client.get(f"{NOAA_BASE_URL}/data", params=params, headers=headers)

                if resp.status_code == 200:
                    data = resp.json().get("results", [])
                    for item in data:
                        records.append({
                            "state": state["name"],
                            "station": item.get("station", ""),
                            "date": item.get("date", "")[:10],
                            "temp_avg": item.get("value") if item.get("datatype") == "TAVG" else None,
                            "precip": item.get("value") if item.get("datatype") == "PRCP" else None,
                            "drought_index": None,
                        })
                    log_api_call(f"NOAA/{state['name']}", resp.status_code, len(data))
                else:
                    log_api_call(f"NOAA/{state['name']}", resp.status_code, 0, resp.text[:200])
            except Exception as e:
                log_api_call(f"NOAA/{state['name']}", 0, 0, str(e)[:200])

    if not records:
        records = seed_weather_data()

    save_weather_data(records)
    return records


# ---------------------------------------------------------------------------
# MULTI-SIGNAL ANOMALY DETECTION ENGINE
# 5 signals cross-referenced for anomaly detection
# ---------------------------------------------------------------------------
def run_anomaly_detection():
    """Run multi-signal anomaly detection across all datasets.

    Signal 1: BENEFIT DEVIATION — z-score of avg benefit per person vs state history
    Signal 2: PARTICIPATION-ECONOMY DIVERGENCE — SNAP enrollment vs unemployment trends
    Signal 3: BENEFIT-INFLATION MISMATCH — benefit changes vs food CPI changes
    Signal 4: QC ERROR RATE RISK — states with high/rising QC error rates
    Signal 5: GEOGRAPHIC OUTLIER — state benefit level vs national peer average
    """
    conn = get_db()
    anomalies = []

    # Load all reference data
    qc_data = load_qc_error_rates()
    unemployment_data = load_unemployment_data()
    cpi_data = load_food_cpi()

    # Build lookup dicts
    qc_by_state = {}
    for q in qc_data:
        key = (q["state"], q["fy"])
        qc_by_state[key] = q

    unemp_by_state = {}
    for u in unemployment_data:
        key = (u["state"], u["year"], u["month"])
        unemp_by_state[key] = u["unemployment_rate"]

    cpi_by_month = {}
    for c in cpi_data:
        cpi_by_month[(c["year"], c["month"])] = c["cpi_food_at_home"]

    # ---- Load SNAP data ----
    rows = conn.execute("""
        SELECT id, state, year, month, participants, benefits_millions, avg_benefit_per_person
        FROM snap_data ORDER BY state, year, month
    """).fetchall()

    state_data = {}
    all_benefits = []
    for row in rows:
        key = row["state"]
        if key not in state_data:
            state_data[key] = []
        d = dict(row)
        state_data[key].append(d)
        if d["avg_benefit_per_person"]:
            all_benefits.append(d["avg_benefit_per_person"])

    # National stats for geographic comparison
    national_mean = sum(all_benefits) / len(all_benefits) if all_benefits else 0
    national_std = (sum((x - national_mean) ** 2 for x in all_benefits) / max(len(all_benefits) - 1, 1)) ** 0.5

    for state, data_points in state_data.items():
        if len(data_points) < 3:
            continue

        avg_ben = [d["avg_benefit_per_person"] for d in data_points if d["avg_benefit_per_person"]]
        parts = [d["participants"] for d in data_points if d["participants"]]
        if not avg_ben:
            continue
        state_mean = sum(avg_ben) / len(avg_ben)
        state_std = (sum((x - state_mean) ** 2 for x in avg_ben) / (len(avg_ben) - 1)) ** 0.5
        if state_std == 0:
            state_std = 0.01  # prevent division by zero, use tiny std
        # Also compute participation stats for spike detection
        part_mean = sum(parts) / len(parts) if parts else 0
        part_std = (sum((x - part_mean) ** 2 for x in parts) / max(len(parts) - 1, 1)) ** 0.5 if len(parts) > 1 else 0

        for dp in data_points:
            if dp["avg_benefit_per_person"] is None:
                continue

            signals = {}
            composite_score = 0
            signal_count = 0

            # SIGNAL 1: Benefit Deviation (z-score within state)
            z_benefit = abs(dp["avg_benefit_per_person"] - state_mean) / state_std
            # Also check participation spike
            z_part = abs(dp["participants"] - part_mean) / part_std if part_std > 0 else 0
            benefit_flag = z_benefit > 1.8
            part_flag = z_part > 2.0
            signals["benefit_deviation"] = {
                "z_score": round(z_benefit, 3),
                "value": dp["avg_benefit_per_person"],
                "mean": round(state_mean, 2),
                "std": round(state_std, 2),
                "flag": benefit_flag or part_flag,
                "participation_z": round(z_part, 3),
                "participation": dp["participants"],
                "part_mean": round(part_mean, 0),
                "part_flag": part_flag,
            }
            if benefit_flag:
                composite_score += min(z_benefit / 4.0, 1.0) * 30  # max 30 pts
                signal_count += 1
            if part_flag:
                composite_score += min(z_part / 4.0, 1.0) * 20  # max 20 pts for participation spike
                signal_count += 1

            # SIGNAL 2: Participation-Economy Divergence
            unemp = unemp_by_state.get((state, dp["year"], dp["month"]))
            if unemp is not None and len(data_points) > 1:
                # Get prior period participation
                prior = [p for p in data_points
                         if (p["year"] * 12 + p["month"]) < (dp["year"] * 12 + dp["month"])]
                if prior:
                    prev = prior[-1]
                    part_change = (dp["participants"] - prev["participants"]) / max(prev["participants"], 1)
                    # Get prior unemployment
                    prev_unemp = unemp_by_state.get((state, prev["year"], prev["month"]))
                    if prev_unemp and prev_unemp > 0:
                        unemp_change = (unemp - prev_unemp) / prev_unemp
                        # Divergence: participation up but unemployment down = suspicious
                        divergence = part_change - unemp_change
                        signals["participation_economy"] = {
                            "participation_change": round(part_change * 100, 2),
                            "unemployment_change": round(unemp_change * 100, 2),
                            "divergence": round(divergence * 100, 2),
                            "flag": divergence > 0.05,  # participation growing 5%+ faster than unemployment
                        }
                        if divergence > 0.05:
                            composite_score += min(divergence * 200, 25)  # max 25 pts
                            signal_count += 1

            # SIGNAL 3: Benefit-Inflation Mismatch
            cpi_current = cpi_by_month.get((dp["year"], dp["month"]))
            # Get CPI from 12 months ago
            cpi_prior = cpi_by_month.get((dp["year"] - 1, dp["month"]))
            if cpi_current and cpi_prior and cpi_prior > 0:
                cpi_change = (cpi_current - cpi_prior) / cpi_prior
                # Get benefit change over same period
                prior_year_ben = [p for p in data_points
                                  if p["year"] == dp["year"] - 1 and p["month"] == dp["month"]]
                if prior_year_ben:
                    ben_change = (dp["avg_benefit_per_person"] - prior_year_ben[0]["avg_benefit_per_person"]) / max(prior_year_ben[0]["avg_benefit_per_person"], 1)
                    mismatch = ben_change - cpi_change
                    signals["benefit_inflation"] = {
                        "benefit_yoy_change": round(ben_change * 100, 2),
                        "food_cpi_yoy_change": round(cpi_change * 100, 2),
                        "mismatch": round(mismatch * 100, 2),
                        "flag": abs(mismatch) > 0.03,  # benefit moving 3%+ differently than CPI
                    }
                    if abs(mismatch) > 0.03:
                        composite_score += min(abs(mismatch) * 300, 20)  # max 20 pts
                        signal_count += 1

            # SIGNAL 4: QC Error Rate Risk
            qc = qc_by_state.get((state, dp["year"]))
            qc_prior = qc_by_state.get((state, dp["year"] - 1))
            if qc:
                error_above_national = qc["total_error_rate"] - 10.93  # FY2024 national
                error_trend = (qc["total_error_rate"] - qc_prior["total_error_rate"]) if qc_prior else 0
                signals["qc_error_rate"] = {
                    "error_rate": qc["total_error_rate"],
                    "overpayment_rate": qc["overpayment_rate"],
                    "above_national": round(error_above_national, 2),
                    "trend": round(error_trend, 2),
                    "flag": error_above_national > 2.0 or error_trend > 1.0,
                }
                if error_above_national > 2.0:
                    composite_score += min(error_above_national * 3, 15)  # max 15 pts
                    signal_count += 1
                if error_trend > 1.0:
                    composite_score += min(error_trend * 5, 10)

            # SIGNAL 5: Geographic Outlier (vs national peers)
            if national_std > 0:
                z_national = abs(dp["avg_benefit_per_person"] - national_mean) / national_std
                signals["geographic_outlier"] = {
                    "z_vs_national": round(z_national, 3),
                    "national_mean": round(national_mean, 2),
                    "flag": z_national > 2.0,
                }
                if z_national > 2.0:
                    composite_score += min(z_national / 3.0, 1.0) * 10  # max 10 pts
                    signal_count += 1

            # Determine if this is a flagged anomaly
            # Flag if: composite > 25 OR benefit z-score alone > 2.5 OR 3+ signals triggered
            should_flag = composite_score > 25 or z_benefit > 2.5 or signal_count >= 3

            if should_flag:
                # Build description
                triggered = [k for k, v in signals.items() if v.get("flag")]
                desc_parts = [f"{state} {dp['year']}-{dp['month']:02d}: avg benefit ${dp['avg_benefit_per_person']:.2f}"]
                desc_parts.append(f"(composite={composite_score:.0f}, signals={signal_count}: {', '.join(triggered)})")

                anomalies.append({
                    "data_source": "snap_data",
                    "record_id": dp["id"],
                    "anomaly_score": round(composite_score, 1),
                    "anomaly_type": "multi_signal" if signal_count >= 2 else ("benefit_spike" if dp["avg_benefit_per_person"] > state_mean else "benefit_drop"),
                    "description": " ".join(desc_parts),
                    "signals": json.dumps(signals),
                })

    # ---- Commodity price anomalies ----
    rows = conn.execute("""
        SELECT id, commodity, price, report_date FROM commodity_prices WHERE price IS NOT NULL
    """).fetchall()

    commodity_data = {}
    for row in rows:
        key = row["commodity"]
        if key not in commodity_data:
            commodity_data[key] = []
        commodity_data[key].append(dict(row))

    for commodity, data_points in commodity_data.items():
        prices = [d["price"] for d in data_points if d["price"]]
        if len(prices) < 3:
            continue
        mean_val = sum(prices) / len(prices)
        std_val = (sum((x - mean_val) ** 2 for x in prices) / (len(prices) - 1)) ** 0.5
        if std_val == 0:
            continue

        for dp in data_points:
            if dp["price"] is None:
                continue
            z_score = abs(dp["price"] - mean_val) / std_val
            if z_score > 1.5:
                anomalies.append({
                    "data_source": "commodity_prices",
                    "record_id": dp["id"],
                    "anomaly_score": round(z_score * 10, 1),  # scale to match composite scoring
                    "anomaly_type": "price_spike" if dp["price"] > mean_val else "price_drop",
                    "description": f"{commodity}: ${dp['price']:.2f} on {dp['report_date']} "
                                   f"(mean ${mean_val:.2f}, z={z_score:.2f})",
                    "signals": json.dumps({"price_deviation": {"z_score": round(z_score, 3), "mean": round(mean_val, 2), "std": round(std_val, 2)}}),
                })

    # ---- Save anomalies ----
    conn.execute("DELETE FROM anomaly_scores")
    for a in anomalies:
        conn.execute(
            "INSERT INTO anomaly_scores (data_source, record_id, anomaly_score, anomaly_type, description, signals) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (a["data_source"], a["record_id"], a["anomaly_score"], a["anomaly_type"], a["description"], a.get("signals", "{}")),
        )
    conn.commit()
    conn.close()
    print(f"[ANOMALY] Detected {len(anomalies)} anomalies ({len([a for a in anomalies if a.get('signals') and 'multi_signal' in a.get('anomaly_type', '')])} multi-signal)", flush=True)
    return anomalies


# ---------------------------------------------------------------------------
# SEED DATA — Fallback when CSVs not available
# ---------------------------------------------------------------------------
def seed_crop_data():
    commodities = [
        {"commodity": "CORN", "unit": "$ / BU", "base": 4.80, "var": 0.60},
        {"commodity": "SOYBEANS", "unit": "$ / BU", "base": 12.50, "var": 1.50},
        {"commodity": "WHEAT", "unit": "$ / BU", "base": 6.40, "var": 0.80},
        {"commodity": "COTTON", "unit": "CENTS / LB", "base": 78.0, "var": 8.0},
        {"commodity": "RICE", "unit": "$ / CWT", "base": 17.20, "var": 2.0},
    ]
    states = ["US TOTAL", "IOWA", "ILLINOIS", "KANSAS", "NEBRASKA", "MINNESOTA",
              "INDIANA", "TEXAS", "NORTH DAKOTA", "CALIFORNIA", "ARKANSAS"]
    records = []
    for c in commodities:
        for year in [2023, 2024, 2025]:
            for state in states:
                state_mod = random.uniform(-0.15, 0.15) * c["base"]
                records.append({
                    "commodity": c["commodity"],
                    "state": state,
                    "year": year,
                    "stat_category": "PRICE RECEIVED",
                    "value": round(c["base"] + random.uniform(-c["var"], c["var"]) + state_mod, 2),
                    "unit": c["unit"],
                })
    return records


def seed_market_data():
    today = datetime.now()
    records = []
    markets = [
        {"commodity": "Live Cattle", "base": 185.0, "var": 12.0, "unit": "$/CWT"},
        {"commodity": "Feeder Cattle", "base": 245.0, "var": 15.0, "unit": "$/CWT"},
        {"commodity": "Lean Hogs", "base": 88.0, "var": 8.0, "unit": "$/CWT"},
        {"commodity": "Corn Futures", "base": 4.65, "var": 0.40, "unit": "$/BU"},
        {"commodity": "Soybean Futures", "base": 12.30, "var": 1.20, "unit": "$/BU"},
        {"commodity": "Wheat Futures", "base": 6.15, "var": 0.70, "unit": "$/BU"},
        {"commodity": "Milk Class III", "base": 19.50, "var": 2.50, "unit": "$/CWT"},
        {"commodity": "Broilers", "base": 1.15, "var": 0.15, "unit": "$/LB"},
    ]
    for m in markets:
        for days_ago in range(30):
            date = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            trend = (30 - days_ago) * random.uniform(-0.005, 0.008) * m["base"]
            records.append({
                "commodity": m["commodity"],
                "price": round(m["base"] + random.uniform(-m["var"], m["var"]) + trend, 2),
                "unit": m["unit"],
                "market": "National",
                "report_date": date,
            })
    return records


def seed_snap_data():
    """Fallback seed data based on published FNS FY2024 state-level statistics."""
    states = [
        {"state": "California", "base_part": 3950000, "base_ben": 234},
        {"state": "Texas", "base_part": 3380000, "base_ben": 181},
        {"state": "Florida", "base_part": 2850000, "base_ben": 175},
        {"state": "New York", "base_part": 2710000, "base_ben": 231},
        {"state": "Illinois", "base_part": 1720000, "base_ben": 197},
        {"state": "Pennsylvania", "base_part": 1640000, "base_ben": 187},
        {"state": "Georgia", "base_part": 1510000, "base_ben": 173},
        {"state": "Ohio", "base_part": 1430000, "base_ben": 183},
        {"state": "Michigan", "base_part": 1300000, "base_ben": 190},
        {"state": "North Carolina", "base_part": 1210000, "base_ben": 171},
    ]
    records = []
    for s in states:
        for year in [2023, 2024, 2025]:
            months = range(1, 13) if year < 2025 else range(1, 4)
            for month in months:
                seasonal = 1.0 + 0.03 * (1 if month in [1, 2, 11, 12] else -1 if month in [6, 7, 8] else 0)
                yearly_trend = 1.0 + (year - 2023) * 0.005
                participants = int(s["base_part"] * seasonal * yearly_trend)
                avg_benefit = round(s["base_ben"] * (1.0 + (year - 2023) * 0.02), 2)
                benefits = round(participants * avg_benefit / 1_000_000, 1)
                records.append({
                    "state": s["state"],
                    "year": year,
                    "month": month,
                    "participants": participants,
                    "benefits_millions": benefits,
                    "avg_benefit_per_person": avg_benefit,
                })
    return records


def seed_weather_data():
    states = [
        {"state": "Iowa", "base_temp": 28, "base_precip": 1.2},
        {"state": "Illinois", "base_temp": 32, "base_precip": 1.5},
        {"state": "Kansas", "base_temp": 35, "base_precip": 0.8},
        {"state": "Nebraska", "base_temp": 30, "base_precip": 0.9},
        {"state": "Minnesota", "base_temp": 22, "base_precip": 1.0},
    ]
    records = []
    today = datetime.now()
    for s in states:
        for days_ago in range(60):
            date = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            month = (today - timedelta(days=days_ago)).month
            seasonal = {1: -15, 2: -10, 3: 0, 4: 15, 5: 25, 6: 35,
                        7: 40, 8: 38, 9: 28, 10: 15, 11: 5, 12: -10}.get(month, 0)
            records.append({
                "state": s["state"],
                "station": f"{s['state'].upper()}_AG_STATION",
                "date": date,
                "temp_avg": round(s["base_temp"] + seasonal + random.uniform(-8, 8), 1),
                "precip": round(max(0, s["base_precip"] + random.uniform(-0.5, 1.5)), 2),
                "drought_index": round(random.uniform(-2.0, 3.0), 2),
            })
    return records


# ---------------------------------------------------------------------------
# DATABASE PERSISTENCE
# ---------------------------------------------------------------------------
def save_crop_data(records):
    conn = get_db()
    conn.execute("DELETE FROM crop_statistics")
    for r in records:
        conn.execute(
            "INSERT INTO crop_statistics (commodity, state, year, stat_category, value, unit) VALUES (?, ?, ?, ?, ?, ?)",
            (r["commodity"], r["state"], r["year"], r["stat_category"], r["value"], r["unit"]),
        )
    conn.commit()
    conn.close()


def save_commodity_prices(records):
    conn = get_db()
    conn.execute("DELETE FROM commodity_prices")
    for r in records:
        conn.execute(
            "INSERT INTO commodity_prices (commodity, price, unit, market, report_date) VALUES (?, ?, ?, ?, ?)",
            (r["commodity"], r.get("price"), r.get("unit"), r.get("market"), r.get("report_date")),
        )
    conn.commit()
    conn.close()


def save_snap_data(records):
    conn = get_db()
    conn.execute("DELETE FROM snap_data")
    for r in records:
        conn.execute(
            "INSERT INTO snap_data (state, year, month, participants, benefits_millions, avg_benefit_per_person) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (r["state"], r["year"], r["month"], r["participants"], r["benefits_millions"], r["avg_benefit_per_person"]),
        )
    conn.commit()
    conn.close()


def save_weather_data(records):
    conn = get_db()
    conn.execute("DELETE FROM weather_data")
    for r in records:
        conn.execute(
            "INSERT INTO weather_data (state, station, date, temp_avg, precip, drought_index) VALUES (?, ?, ?, ?, ?, ?)",
            (r["state"], r["station"], r["date"], r.get("temp_avg"), r.get("precip"), r.get("drought_index")),
        )
    conn.commit()
    conn.close()
