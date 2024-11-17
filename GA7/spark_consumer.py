from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, to_timestamp, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType



def create_spark_session():
    return (
        SparkSession.builder.appName("SalesDataConsumer")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0"
        )
        .getOrCreate()
    )

def create_schema():
    return StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
        ]
    )

def start_streaming():
    spark = create_spark_session()
    schema = create_schema()

    # Read from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "sales_data")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON data and convert timestamp to timestamp type
    parsed_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

    # Calculate counts over 10-second windows
    windowed_counts = (
        parsed_df.withWatermark("timestamp", "10 seconds")
        .groupBy(window("timestamp", "10 seconds", "5 seconds"))
        .agg(count("*").alias("record_count"))
    )

    # Start the streaming query
    query = (
        windowed_counts.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    start_streaming()