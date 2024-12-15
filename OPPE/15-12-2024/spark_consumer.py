from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lag
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


spark = SparkSession.builder.appName("stock_anomaly_detection").getOrCreate()

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock_data")
    .load()
)

schema = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("company", StringType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
    ]
)

stock_data = df.selectExpr("CAST(value AS STRING) as json_string")

window_spec = Window.partitionBy("company").orderBy("timestamp").rowsBetween(-10, -1)

stock_data = (
    stock_data
        .withColumn(
            "rolling_avg_volume", 
            avg(col("volume")).over(window_spec)
        )
        .withColumn(
            "prev_close",
            lag(col("close")).over(Window.partitionBy("company").orderBy("timestamp")),
        )
        .withColumn(
            "price_change_percent", (col("close") - col("prev_close")) / col("prev_close") * 100
        )
        .withColumn(
            "anomaly_flag",
            (col("price_change_percent") > 0.5) | (col("volume") > col("rolling_avg_volume") * 1.02),
        )
)

anomalies = stock_data.filter(col("anomaly_flag"))
query = anomalies.writeStream.outputMode("append").format("console").start()

query.awaitTermination()


# SQL query

# WITH cte as (
#     SELECT company,
#            timestamp,
#            close,
#            volume,
#            LAG(close) OVER(PARTITION BY company ORDER BY timestamp) as prev_close,
#            AVG(volume) OVER(PARTITION BY company ORDER BY timestamp ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as rolling_avg_volume
#     FROM   stock_data
# )
# ,anomalies_cte as (
#     SELECT company,
#            timestamp,
#            close,
#            volume,
#            (close - prev_close) / prev_close * 100 as price_chnage_percent,
#     FROM   stock_data_with_lag
#     WHERE  (close - prev_close) / prev_close * 100 > 0.5
#             or volume > avg_volume * 1.02
# )
