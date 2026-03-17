# USDA Agricultural AI Data Dashboard

Real-time agricultural market data aggregation with AI-powered anomaly detection for fraud and improper payment analysis. Built to support USDA's AI Strategy (FY26–27) focus areas.

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

Uses z-score statistical analysis across SNAP benefit data and commodity prices to flag potential improper payments and market anomalies. Designed with human-in-the-loop review per USDA AI Strategy and OMB M-25-21 requirements.
