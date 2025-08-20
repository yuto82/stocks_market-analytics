import json
import websocket
from kafka import KafkaProducer
from analytical_pipeline.config.settings import Config
from analytical_pipeline.utils.logger import twelvedata_logger

class TwelveDataWebSocketClient:
    def __init__(self):
        self.ws = None
        self.twelvedata_api_key = Config.TWELVE_DATA_API_KEY
        self.twelvedata_url = Config.TWELVE_DATA_WS_URL
        self.symbols = Config.STOCK_SYMBOLS
        self.logger = twelvedata_logger  
        self._setup_kafka_producer()     

    def _setup_kafka_producer(self):
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            self.logger.info("Kafka producer initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to setup Kafka producer: {e}")
            self.kafka_producer = None

    def send_to_kafka(self, data: dict, symbol: str):
        if self.kafka_producer:
            try:
                topic = Config.KAFKA_TOPICS["quotes"]
                self.kafka_producer.send(topic, value=data, key=symbol)
                self.logger.debug(f"Sent {symbol} to Kafka: {data}")
            except Exception as e:
                self.logger.error(f"Failed to send to Kafka: {e}")

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            symbol = data.get('symbol', 'unknown')
            self.send_to_kafka(data, symbol)
            self.logger.info(f"Processed message for {symbol}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
        
    def on_error(self, ws, error):
        print("Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("Connection is closed.")
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()

    def on_open(self, ws):
        print("Connection is opened.")
        payload = {
            "action": "subscribe",
            "params": {
                "symbols": self.symbols,
                "apikey": self.twelvedata_api_key
            }
        }
        ws.send(json.dumps(payload))

    def run(self):
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(
            url = f"{self.twelvedata_url}?apikey={self.twelvedata_api_key}",
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.run_forever()

if __name__ == "__main__":
    client = TwelveDataWebSocketClient()
    client.run()