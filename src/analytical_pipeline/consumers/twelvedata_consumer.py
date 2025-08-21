from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from analytical_pipeline.config.settings import Config

if __name__ == '__main__':
    spark = (
        SparkSession.builder
        .appName("TwelveDataConsumer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3")
        .config("spark.sql.adaptive.enabled", "false")  
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    sample_df = (
        spark.readStream 
        .format("Kafka") 
        .option("kafka.bootstrap.servers", "localhost:9092") 
        .option("subscribe", "stock-quotes") 
        .option("startingOffsets", "latest") 
        .load()
    )

    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("timestamp", LongType()),
        StructField("message_type", StringType()),
        StructField("source", StringType()),
        StructField("processed_at", StringType())
    ])

    parsed_df = sample_df.selectExpr(
        "CAST(key AS STRING) as symbol",
        "CAST(value AS STRING) as raw_value"
    )

    final_df = parsed_df.withColumn("data", from_json(col("raw_value"), schema)) \
                       .select("symbol", "data.*")

    console_query = (
        final_df.writeStream
        .format("console")
        .option("truncate", False)
        .outputMode("append")
        .start()
    )

    console_query.awaitTermination()