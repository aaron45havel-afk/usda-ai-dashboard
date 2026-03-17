# USDA Agricultural AI Data Dashboard

Agricultural market data aggregation with multi-signal anomaly detection across FNS/SNAP benefits and USDA commodity markets. Proof of concept.

## Data Sources (3 Tiers)

| Tier | Source | Data |
|------|--------|------|
| 1 | USDA NASS QuickStats | Crop prices, production stats, farm economics |
| 1 | USDA FNS | SNAP participation, benefits, state-level data |
| 2 | USDA AMS Market News | Live livestock, grain, dairy market prices |
| 3 | NOAA Climate Data | Temperature, precipitation, drought indices |

## Quick Start

```bash
# Clone and install
git clone https://github.com/YOUR_USER/usda-ai-dashboard.git
cd usda-ai-dashboard
pip install -r requirements.txt

# Configure API keys (optional — works with seed data without keys)
cp .env.example .env
# Edit .env with your API keys

# Run locally
python main.py
# Open http://localhost:8000
```

## Deploy to Railway

1. Push to GitHub
2. Connect repo at [railway.app](https://railway.app)
3. Add environment variables: `NASS_API_KEY`, `NOAA_API_TOKEN`
4. Deploy — Railway auto-detects the Dockerfile

## API Keys (Free)

- **USDA NASS**: https://quickstats.nass.usda.gov/api (instant)
- **NOAA CDO**: https://www.ncdc.noaa.gov/cdo-web/token (instant)

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/overview` | Dashboard metrics |
| `GET /api/commodities` | All commodity prices |
| `GET /api/commodities/{name}` | Single commodity detail |
| `GET /api/snap` | SNAP participation & benefits |
| `GET /api/weather` | Weather data for farm belt |
| `GET /api/anomalies` | AI anomaly detection results |
| `GET /api/crop-stats` | NASS crop statistics |
| `POST /api/refresh` | Trigger data refresh |
| `GET /api/health` | Health check |

## Anomaly Detection

Uses a 5-signal engine (benefit deviation, participation-economy divergence, benefit-inflation mismatch, QC error rate risk, geographic outlier) to flag unusual SNAP patterns at the state-month level. Commodity prices are scored separately via z-score analysis. All flags are surfaced for human review — no automated action is taken.
