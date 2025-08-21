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

    df = sample_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    query = (
        df.writeStream
        .format("console")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()