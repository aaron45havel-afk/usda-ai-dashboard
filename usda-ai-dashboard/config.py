import os
from dotenv import load_dotenv

load_dotenv()

NASS_API_KEY = os.getenv("NASS_API_KEY", "")
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN", "")
PORT = int(os.getenv("PORT", 8000))
HOST = os.getenv("HOST", "0.0.0.0")

# API base URLs
NASS_BASE_URL = "https://quickstats.nass.usda.gov/api"
AMS_BASE_URL = "https://marsapi.ams.usda.gov/services/v1.2"
FNS_SNAP_URL = "https://api.usa.gov/USDA/FNS"
NOAA_BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./data/usda_data.db")
