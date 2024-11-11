from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window


current_date = datetime.now().strftime("%Y-%m-%d")

# Initialize Spark session
spark = SparkSession.builder.appName("SCD_Type_2").getOrCreate()

# Access data
original = spark.read.csv(
    "gs://iitm-ibd-ga5/original_data.csv", header=True, inferSchema=True
)
updated = spark.read.csv(
    "gs://iitm-ibd-ga5/updated_data.csv", header=True, inferSchema=True
)

# Define window for finding the last idx
windowSpec = Window.orderBy(F.col("idx").desc())


# Step 1: Update 'end_date' in the original table where the name matches and end_date is greater than the current date

original = (
    original.alias("orig")
    .join(updated.select("name").distinct().alias("upd"), on="name", how="left")
    .withColumn(
        "end_date",
        F.when(
            (F.col("upd.name").isNotNull()) & (F.col("orig.end_date") > current_date),
            current_date,
        ).otherwise(F.col("orig.end_date")),
    )
    .select("orig.*")
)

# Step 2: Append new rows from the updated table to the original table with new indices
# Get the maximum idx from the original data

max_idx = original.select(F.max("idx").alias("max_idx")).collect()[0]["max_idx"]

# Create new records from the updated dataset
new_records = (
    updated.withColumn("idx", F.row_number().over(Window.orderBy("name")) + max_idx)
    .withColumn("start_date", F.lit(current_date))
    .withColumn("end_date", F.lit("9999-12-31"))
)

# Union the original and new records
original = original.union(new_records.select(original.columns))

# Show the data
original.show()

# Save to CSV in GCS-compatible format
output_path = "gs://iitm-ibd-ga5/output.csv"
original.write.csv(output_path, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
