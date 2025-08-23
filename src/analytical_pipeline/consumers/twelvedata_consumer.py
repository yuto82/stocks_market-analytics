from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

import sys
import signal
from dataclasses import dataclass
from typing import Dict, List, Optional

from analytical_pipeline.config.settings import Config
from analytical_pipeline.utils.logger import spark_logger

from kafka.admin import KafkaAdminClient

@dataclass
class StockMessage:
    symbol: str
    price: float
    volume: float
    timestamp: str
    message_type: str
    source: str
    processed_at: str

    @classmethod
    def schema(cls) -> StructType:
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("message_type", StringType(), False),
            StructField("source", StringType(), False),
            StructField("processed_at", StringType(), False),
        ])

class TwelveDataConsumer:
    def __init__(self):
        self.logger = spark_logger

        self.spark: Optional[SparkSession] = None
        self.active_queries: List[StreamingQuery] = []
        
        self.checkpoint_location = Config.CHECKPOINT_LOCATION
        self.kafka_topics = Config.KAFKA_TOPICS

        self._setup_signal_handlers()
        self._running = True

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
    
    def _handle_signal(self, signum, frame):
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._running = False
        self.stop_all_streams()

    def _create_spark_session(self) -> SparkSession:
        try:
            spark = (
                SparkSession.builder
                .appName("TwelveDataConsumer")
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3")
                .config("spark.sql.adaptive.enabled", "true")  
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.streaming.stateStore.maintenanceInterval", "600s")
                .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
                .getOrCreate() 
            )

            spark.sparkContext.setLogLevel("WARN")
            self.logger.info("Spark session created successfully")
            return spark
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {e}")
            raise

    def _create_kafka_stream(self, topics: List[str], starting_offsets: str = "latest") -> DataFrame:
        try:
            kafka_options = {
                "kafka.bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
                "subscribe": ",".join(topics),
                "startingOffsets": starting_offsets,
                "failOnDataLoss": "false",
                "maxOffsetsPerTrigger": "1000",
                "kafka.consumer.group.id": "twelvedata-consumer-group",
                "kafka.session.timeout.ms": "30000",
                "kafka.request.timeout.ms": "40000",
            }
            
            df = (
                self.spark.readStream
                .format("kafka")
                .options(**kafka_options)
                .load()
            )
            
            self.logger.info(f"Created Kafka stream for topics: {topics}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to create Kafka stream: {e}")
            raise

    def _get_existing_topics(self, requested_topics: List[str]) -> List[str]:
        try:
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                client_id='topic-checker'
            )
            
            all_topics = admin_client.list_topics()
            admin_client.close()
            
            existing_topics = [topic for topic in requested_topics if topic in all_topics]
            
            if existing_topics:
                self.logger.info(f"Found existing topics: {existing_topics}")
            else:
                self.logger.warning("No existing topics found from requested list")
            
            non_existing = [topic for topic in requested_topics if topic not in all_topics]
            if non_existing:
                self.logger.warning(f"Topics not found in Kafka: {non_existing}")
            
            return existing_topics
            
        except Exception as e:
            self.logger.error(f"Failed to check existing topics: {e}")
            return []

    def _parse_kafka_messages(self, df: DataFrame, schema: StructType = StockMessage.schema()) -> DataFrame:
        try:
            parsed_df = df.selectExpr(
                "CAST(key AS STRING) as kafka_key",
                "CAST(value AS STRING) as raw_value",
                "topic",
                "partition", 
                "offset",
                "timestamp as kafka_timestamp"
            )
            
            enriched_df = (
                parsed_df
                .withColumn("data", from_json(col("raw_value"), schema))
                .select(
                    "kafka_key",
                    "topic", 
                    "partition",
                    "offset",
                    "kafka_timestamp",
                    "data.*" 
                )
            )
            
            final_df = (
                enriched_df
                .withColumn("spark_processed_at", current_timestamp())
                .withColumn("is_valid_price", col("price").isNotNull() & (col("price") > 0))
                .withColumn("is_heartbeat", col("message_type") == "heartbeat")
                .withColumn("is_price_update", col("message_type") == "price")
            )
            
            self.logger.info("Successfully parsed Kafka messages")
            return final_df
        except Exception as e:
            self.logger.error(f"Failed to parse Kafka messages: {e}")
            raise
        
    def start_console_output(self, df: DataFrame, query_name: str = "console_output") -> StreamingQuery:
        try:
            query = (
                df.writeStream
                .queryName(query_name)
                .format("console")
                .option("truncate", False)
                .option("numRows", 20)
                .outputMode("append")
                .trigger(processingTime="10 seconds")
                .start()
            )
            
            self.active_queries.append(query)
            self.logger.info(f"Started console output stream: {query_name}")
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to start console output: {e}")
            raise
    
    def run_streaming_pipeline(self, enable_console: bool = True):
        try:
            self.spark = self._create_spark_session()
            
            requested_topics = list(self.kafka_topics.values())
            existing_topics = self._get_existing_topics(requested_topics)
            
            if not existing_topics:
                self.logger.error("No existing topics found. Cannot start streaming pipeline.")
                return
            
            self.logger.info(f"Starting streaming pipeline for existing topics: {existing_topics}")
            kafka_stream = self._create_kafka_stream(existing_topics)
            parsed_stream = self._parse_kafka_messages(kafka_stream)
            
            if enable_console:
                self.start_console_output(parsed_stream, "main_console")
            
            self.logger.info("All streaming queries started. Waiting for termination...")
            self.await_termination()
            
        except Exception as e:
            self.logger.error(f"Error in streaming pipeline: {e}")
            self.stop_all_streams()
            raise

    def await_termination(self):
        try:
            while self._running and self.active_queries:
                for query in self.active_queries:
                    if not query.isActive:
                        self.active_queries.remove(query)
                        self.logger.warning(f"Query {query.name} terminated")
                
                if self.active_queries:
                    self.spark.sparkContext.setJobGroup("streaming_monitor", "Monitor streaming queries")
                    import time
                    time.sleep(5)
                    
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"Error while waiting for termination: {e}")
        finally:
            self.stop_all_streams()

    def stop_all_streams(self):
        self.logger.info("Stopping all streaming queries...")
        
        for query in self.active_queries:
            try:
                if query.isActive:
                    query.stop()
                    self.logger.info(f"Stopped query: {query.name}")
            except Exception as e:
                self.logger.error(f"Error stopping query {query.name}: {e}")
        
        self.active_queries.clear()
        
        if self.spark:
            try:
                self.spark.stop()
                self.logger.info("Spark session stopped")
            except Exception as e:
                self.logger.error(f"Error stopping Spark session: {e}")

    def get_stream_status(self) -> Dict:
        status = {
            'total_queries': len(self.active_queries),
            'active_queries': [],
            'inactive_queries': []
        }
        
        for query in self.active_queries:
            query_info = {
                'name': query.name,
                'id': query.id,
                'is_active': query.isActive,
                'last_progress': query.lastProgress
            }
            
            if query.isActive:
                status['active_queries'].append(query_info)
            else:
                status['inactive_queries'].append(query_info)
        
        return status


def main():
    consumer = TwelveDataConsumer()
    try:
        consumer.run_streaming_pipeline()
    except KeyboardInterrupt:
        consumer.logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        consumer.logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        consumer.stop_all_streams()

if __name__ == "__main__":
    main()