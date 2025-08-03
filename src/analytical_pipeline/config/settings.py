import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Polygon.io
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
    POLYGON_BASE_URL = os.getenv("POLYGON_BASE_URL", "https://api.polygon.io")

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPICS = {
        "trades": "stock-trades",
        "quotes": "stock-quotes",
        "aggregates": "stock-aggregates"
    }

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate_required_env_variables(cls):
        required_variables = [
            "POLYGON_API_KEY",
        ]

        missing_variables = []

        for variable in required_variables:
            if not getattr(cls, variable):
                missing_variables.append(variable)
        
        if missing_variables:
            raise ValueError(f"Missing variables: {missing_variables}")
        
        return True