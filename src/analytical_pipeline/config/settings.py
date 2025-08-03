import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
    POLYGON_BASE_URL = os.getenv("POLYGON_BASE_URL", "https://api.polygon.io")
    