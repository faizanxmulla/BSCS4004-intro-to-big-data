from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when

spark = SparkSession.builder.appName("user_click_counter").getOrCreate()
input_file = "gs://iitm-ibd-ga3/data.csv"
output_file = "output.txt"

data = spark.read.option("header", "true").option("delimiter", ",").csv(input_file)

data = data.withColumn("Hour", hour(data["timestamp"]))

data = data.withColumn(
    "time_interval",
    when(col("Hour") < 6, "00-06")
    .when(col("Hour") < 12, "06-12")
    .when(col("Hour") < 18, "12-18")
    .when(col("Hour") < 24, "18-24")
    .otherwise("Invalid timestamp"),
)

data = data.sort("time_interval")
result = data.groupBy("time_interval").count().sort("time_interval")
result.show()
result.toPandas().to_csv(output_file, sep="\t", index=False, header=True)

spark.stop()
