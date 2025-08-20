import sys
import json
import time
import signal
import websocket
from enum import Enum
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from dataclasses import dataclass, asdict

from kafka import KafkaProducer
from kafka.errors import KafkaError

from analytical_pipeline.config.settings import Config
from analytical_pipeline.utils.logger import twelvedata_logger

class MessageType(Enum):
    """
    Enum class representing the types of messages received from the TwelveData WebSocket API.

    This enum is used to standardize handling of different message types such as 
    price updates, system messages, and errors.

    Attributes:
        PRICE (str): Message containing the price of a stock or cryptocurrency.
        HEARTBEAT (str): System message used to check that the connection is still alive.
        STATUS (str): Message indicating subscription or connection status.
        ERROR (str): Message indicating an error from the server (e.g., invalid API key).
        UNKNOWN (str): Default type for unrecognized messages.
    """
    PRICE = "price"
    HEARTBEAT = "heartbeat" 
    STATUS = "status"
    ERROR = "error"
    UNKNOWN = "unknown"

@dataclass
class StockMessage:
    """
    Standardized representation of a stock or trading message from a data source.

    Attributes:
        symbol (str): The stock or asset ticker (e.g., "AAPL", "BTC/USD").
        price (Optional[float]): The latest price of the asset, if available.
        volume (Optional[float]): The trading volume associated with the message, if available.
        timestamp (Optional[str]): The original timestamp of the message from the data source (ISO 8601 format).
        message_type (str): Type of message, e.g., price, heartbeat, status, error, or unknown. Defaults to 'unknown'.
        source (str): The source of the data message. Defaults to "twelvedata".
        processed_at (str): UTC timestamp indicating when the message was processed locally. Automatically set if not provided.

    Methods:
        __post_init__():
            Automatically sets `processed_at` to the current UTC time in ISO 8601 format if it is not provided.
    """
    symbol: str
    price: Optional[float] = None
    volume: Optional[float] = None
    timestamp: Optional[str] = None
    message_type: str = MessageType.UNKNOWN.value
    source: str = "twelvedata"
    processed_at: str = None
    
    def __post_init__(self):
        if self.processed_at is None:
            self.processed_at = datetime.now(timezone.utc).isoformat()


class KafkaMessageRouter:
    """
    Routes StockMessage objects to appropriate Kafka topics based on message type.

    This class centralizes the logic of determining which Kafka topic to send a message to,
    handles serialization of messages, sends them using a KafkaProducer, and logs the delivery 
    status or errors.

    Attributes:
        producer (KafkaProducer): Kafka producer instance used to send messages.
        topics (Dict[str, str]): Mapping of logical message types to Kafka topic names.
        logger: Logger instance for logging delivery information and errors.

    Methods:
        route_message(message: StockMessage) -> bool:
            Sends a StockMessage to the appropriate Kafka topic.
            Returns True if delivery is successful, False otherwise.

        _get_topic_for_message(message: StockMessage) -> str:
            Determines the correct Kafka topic for a given message based on its type.
    """

    def __init__(self, producer: KafkaProducer, topics: Dict[str, str]):
        """
        Initializes KafkaMessageRouter with a Kafka producer and topic mapping.

        Args:
            producer (KafkaProducer): Kafka producer used for sending messages.
            topics (Dict[str, str]): Dictionary mapping message types to Kafka topic names.
        """
        self.producer = producer
        self.topics = topics
        self.logger = twelvedata_logger
        
    def route_message(self, message: StockMessage) -> bool:
        """
        Routes a StockMessage to the appropriate Kafka topic.

        The message is serialized to a dictionary and sent with the symbol as the key.
        Delivery confirmation is awaited with a timeout. Logs success or errors.

        Args:
            message (StockMessage): The stock message to route.

        Returns:
            bool: True if message is successfully delivered, False if an error occurs.
        """
        try:
            topic = self._get_topic_for_message(message)
            key = message.symbol
            value = asdict(message)
            
            future = self.producer.send(
                topic=topic,
                key=key,
                value=value
            )
            
            record_metadata = future.get(timeout=10)
            
            self.logger.info(
                f"Message delivered: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}, symbol={message.symbol}"
            )
            return True
            
        except KafkaError as e:
            self.logger.error(f"Kafka delivery failed: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error in message routing: {e}")
            return False
    
    def _get_topic_for_message(self, message: StockMessage) -> str:
        """
        Determines the Kafka topic based on the StockMessage type.

        Maps message types (price, heartbeat, status) to configured Kafka topics.
        Defaults to the 'quotes' topic if message type is unknown.

        Args:
            message (StockMessage): The stock message for which to determine the topic.

        Returns:
            str: Kafka topic name.
        """
        message_type_to_topic = {
            MessageType.PRICE.value: self.topics.get("quotes", "stock-quotes"),
            MessageType.HEARTBEAT.value: self.topics.get("heartbeat", "stock-heartbeat"),
            MessageType.STATUS.value: self.topics.get("status", "stock-status"),
        }
        
        return message_type_to_topic.get(
            message.message_type, 
            self.topics.get("quotes", "stock-quotes")
        )
    
class MessageParser:    
    @staticmethod
    def parse(raw_message: str) -> Optional[StockMessage]:
        try:
            data = json.loads(raw_message)
            
            if MessageParser._is_heartbeat(data):
                return StockMessage(
                    symbol="HEARTBEAT",
                    message_type=MessageType.HEARTBEAT.value
                )
            
            if MessageParser._is_status_message(data):
                return StockMessage(
                    symbol=data.get("symbol", "STATUS"),
                    message_type=MessageType.STATUS.value
                )
                
            symbol = MessageParser._extract_symbol(data)
            if not symbol:
                return None
                
            return StockMessage(
                symbol=symbol,
                price=MessageParser._extract_price(data),
                volume=MessageParser._extract_volume(data),
                timestamp=MessageParser._extract_timestamp(data),
                message_type=MessageType.PRICE.value
            )
            
        except (json.JSONDecodeError, KeyError) as e:
            twelvedata_logger.error(f"Failed to parse message: {e}")
            return None
    
    @staticmethod
    def _extract_symbol(data: Dict[str, Any]) -> Optional[str]:
        possible_fields = ["symbol", "s", "ticker", "instrument"]
        for field in possible_fields:
            if symbol := data.get(field):
                return str(symbol).upper()
        return None
    
    @staticmethod
    def _extract_price(data: Dict[str, Any]) -> Optional[float]:
        possible_fields = ["price", "p", "last", "close"]
        for field in possible_fields:
            if price := data.get(field):
                try:
                    return float(price)
                except (ValueError, TypeError):
                    continue
        return None
    
    @staticmethod
    def _extract_volume(data: Dict[str, Any]) -> Optional[float]:
        possible_fields = ["volume", "v", "vol"]
        for field in possible_fields:
            if volume := data.get(field):
                try:
                    return float(volume)
                except (ValueError, TypeError):
                    continue
        return None
    
    @staticmethod
    def _extract_timestamp(data: Dict[str, Any]) -> Optional[str]:
        possible_fields = ["timestamp", "t", "time", "ts"]
        for field in possible_fields:
            if timestamp := data.get(field):
                return str(timestamp)
        return None
    
    @staticmethod
    def _is_heartbeat(data: Dict[str, Any]) -> bool:
        heartbeat_indicators = ["heartbeat", "ping", "keepalive"]
        return any(indicator in str(data).lower() for indicator in heartbeat_indicators)
    
    @staticmethod
    def _is_status_message(data: Dict[str, Any]) -> bool:
        status_indicators = ["status", "connection", "subscribed", "error"]
        return any(indicator in str(data).lower() for indicator in status_indicators)

class TwelveDataWebSocketClient:
    def __init__(self):
        self.ws: Optional[websocket.WebSocketApp] = None
        self.kafka_producer: Optional[KafkaProducer] = None
        self.message_router: Optional[KafkaMessageRouter] = None
        self.logger = twelvedata_logger
        
        self.api_key = Config.TWELVE_DATA_API_KEY
        self.ws_url = Config.TWELVE_DATA_WS_URL
        self.symbols = ",".join(Config.STOCK_SYMBOLS)
        
        self.stats = {
            'messages_received': 0,
            'messages_sent_to_kafka': 0,
            'messages_failed': 0,
            'connection_errors': 0,
            'start_time': time.time()
        }
        
        self._setup_signal_handlers()
        self._running = True
        
    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._running = False
        self.stop()
    
    def _setup_kafka_producer(self) -> bool:
        try:
            producer_config = {
                'bootstrap_servers': Config.KAFKA_BOOTSTRAP_SERVERS.split(','),
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None,
                # 'acks': 'all',  
                # 'retries': 3,
                # 'retry_backoff_ms': 1000,
                # 'request_timeout_ms': 30000,
                # 'delivery_timeout_ms': 120000,
                # 'batch_size': 16384,
                # 'linger_ms': 10,
                # 'compression_type': 'gzip',
                # 'max_in_flight_requests_per_connection': 5,
                # 'enable_idempotence': True
            }
            
            self.kafka_producer = KafkaProducer(**producer_config)
            self.message_router = KafkaMessageRouter(
                self.kafka_producer, 
                Config.KAFKA_TOPICS
            )
            self.logger.info("Kafka producer initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup Kafka producer: {e}")
            return False
    
    def _log_statistics(self):
        runtime = time.time() - self.stats['start_time']
        self.logger.info(
            f"Statistics - Runtime: {runtime:.1f}s, "
            f"Received: {self.stats['messages_received']}, "
            f"Sent to Kafka: {self.stats['messages_sent_to_kafka']}, "
            f"Failed: {self.stats['messages_failed']}, "
            f"Connection errors: {self.stats['connection_errors']}"
        )
    
    def on_message(self, ws, message: str):

        print(f"RAW: {message}")
        
        self.stats['messages_received'] += 1
        
        try:
            self.logger.debug(f"Raw message: {message}")
            
            parsed_message = MessageParser.parse(message)
            if not parsed_message:
                self.logger.warning("Failed to parse message, skipping")
                self.stats['messages_failed'] += 1
                return
            
            if self.message_router:
                success = self.message_router.route_message(parsed_message)
                if success:
                    self.stats['messages_sent_to_kafka'] += 1
                else:
                    self.stats['messages_failed'] += 1
            
            if self.stats['messages_received'] % 100 == 0:
                self._log_statistics()
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            self.stats['messages_failed'] += 1
    
    def on_error(self, ws, error):
        self.stats['connection_errors'] += 1
        self.logger.error(f"WebSocket error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        self.logger.info(
            f"WebSocket connection closed - Code: {close_status_code}, "
            f"Message: {close_msg}"
        )
        self._cleanup_resources()
    
    def on_open(self, ws):
        self.logger.info("WebSocket connection opened")
        payload = {
            "action": "subscribe",
            "params": {
                "symbols": self.symbols,
                "apikey": self.api_key
            }
        }
        
        try:
            ws.send(json.dumps(payload))
            self.logger.info(f"Subscribed to symbols: {self.symbols}")
        except Exception as e:
            self.logger.error(f"Failed to send subscription: {e}")
    
    def _cleanup_resources(self):
        try:
            if self.kafka_producer:
                self.logger.info("Flushing Kafka producer...")
                self.kafka_producer.flush(timeout=30)
                self.kafka_producer.close(timeout=30)
                self.logger.info("Kafka producer closed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
        
        self._log_statistics()
    
    def start(self) -> bool:
        if not self._setup_kafka_producer():
            self.logger.error("Failed to setup Kafka producer, aborting...")
            return False
        
        try:
            websocket.enableTrace(False)
            self.ws = websocket.WebSocketApp(
                url=f"{self.ws_url}?apikey={self.api_key}",
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            
            self.logger.info("Starting WebSocket client...")
            self.ws.run_forever()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start WebSocket client: {e}")
            return False
    
    def stop(self):
        self._running = False
        if self.ws:
            self.ws.close()
        self._cleanup_resources()


def main():
    client = TwelveDataWebSocketClient()
    
    try:
        success = client.start()
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        client.logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        client.logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        client.stop()

if __name__ == "__main__":
    main()