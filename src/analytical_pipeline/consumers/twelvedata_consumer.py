from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

import signal
from typing import Dict, List, Optional, Callable

from analytical_pipeline.config.settings import Config
from analytical_pipeline.utils.logger import spark_logger

class TwelveDataConsumer:
    def __init__(self):
        self.spark: Optional[SparkSession]
        self.logger = spark_logger

        self.checkpoint_location = Config.CHECKPOINT_LOCATION

        self._setup_signal_handlers()
        self._running = True

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
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

    def _create_kafka_stream(self, topics: List[str]) -> DataFrame:
        try:
            kafka_options = {
                "kafka.bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
                "subscribe": "stock-quotes", #это поменять
                "startingOffsets": "latest",    #это тоже
                "failOnDataLoss": "false",
                "maxOffsetsPerTrigger": "1000",
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

    def _parse_kafka_messages(self, df: DataFrame) -> DataFrame:
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
                .withColumn("data", from_json(col("raw_value"), self.stock_message_schema))
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