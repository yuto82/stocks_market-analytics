import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Polygon.io
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
    POLYGON_WS_URL = os.getenv("POLYGON_WS_URL", "")

    #Twelve Data
    TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
    TWELVE_DATA_WS_URL = os.getenv("TWELVE_DATA_WS_URL", "wss://ws.twelvedata.com/v1/quotes/price")

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPICS = {
        "trades": "stock-trades",
        "quotes": "stock-quotes",
        "aggregates": "stock-aggregates"
    }
    CHECKPOINT_LOCATION = "/Users/ramanhrytsal/Desktop/stocks_market_analytics/stocks_market-analytics/src/analytical_pipeline/consumers/checkpoint"

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    PRODUCER_LOGGER = "producer_logger"
    CONSUMER_LOGGER = "consumer_logger"

    # Symbols
    # STOCK_SYMBOLS = ["BTC/USD"]
    STOCK_SYMBOLS = ["AAPL"]

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