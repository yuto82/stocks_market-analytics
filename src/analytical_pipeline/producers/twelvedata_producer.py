import json
import websocket
import threading
from kafka import KafkaProducer
from ..config.settings import Config
from ..utils.logger import twelvedata_logger


class TwelveDataProducer:
    def __init__(self):
        self.ws = None
        self.kafka_producer = None
        self.is_connected = False
        self.is_authenticated = False
        
        self.api_key = Config.TWELVE_DATA_API_KEY
        self.symbols = Config.STOCK_SYMBOLS
        self.ws_url = Config.TWELVE_DATA_WS_URL
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
                self.logger.debug(f"Sent {symbol} price to Kafka: {data['price']}")
            except Exception as e:
                self.logger.error(f"Failed to send to Kafka: {e}")


    def connect(self):
        try:
            ws_url_with_auth = f"{self.ws_url}?apikey={self.api_key}"
            
            self.ws = websocket.WebSocketApp(
                ws_url_with_auth,
                on_message=self.on_message,
                on_error=self.on_error,
                on_open=self.on_open,
                on_close=self.on_close
            )
            
            self.logger.info("Connecting to Twelve Data WebSocket...")
            self.ws.run_forever()
            
        except Exception as e:
            self.logger.error(f"Failed to connect to WebSocket: {e}")
            self.is_connected = False
    
    def on_open(self, ws):
        self.logger.info("WebSocket connection opened")
        self.is_connected = True
        self._subscribe_to_symbols()

    def on_close(self, ws, close_status_code, close_msg):
        self.logger.warning(f"WebSocket connection closed: {close_msg}")
        self.is_connected = False

    def _subscribe_to_symbols(self):
        if self.ws and self.is_connected:
            symbols_string = ",".join(self.symbols)
            
            subscription_msg = {
                "action": "subscribe",
                "params": {
                    "symbols": symbols_string
                }
            }
            
            self.ws.send(json.dumps(subscription_msg))
            self.logger.info(f"Subscribed to symbols: {symbols_string}")


    def on_message(self, ws, message):
        # Обработка сообщений
        pass
    
    def on_error(self, ws, error):
        # Error handling
        pass
    
    def run(self):
        # Main loop
        pass