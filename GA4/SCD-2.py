from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import when


current_date = datetime.now().strftime("%d-%m-%Y")

# Create a spark session
spark = SparkSession.builder.appName("SCD_Type_2").getOrCreate()

# Access data
original = spark.read.csv("gs://iitm-ibd-ga4/original_data.csv", header=True, inferSchema=True)
updated = spark.read.csv("gs://iitm-ibd-ga4/updated_data.csv", header=True, inferSchema=True)

for row in updated.collect():
    condition1 = F.col("name") == row.name
    condition2 = F.col("end_date") > current_date

    original = original.withColumn(
        "end_date",
        when(condition1 & condition2, current_date).otherwise(original.end_date),
    )

for row in updated.collect():
    idx_ = original.tail(1)[0].idx + 1
    name_ = row.name
    dob_ = row.dob
    tuple_ = (idx_, name_, dob_, current_date, "10-12-2099")

    row_ = spark.createDataFrame([tuple_], schema=original.schema)
    original = original.union(row_)

# Show the data
original.show()

# Save to CSV and stop the spark session
original.toPandas().to_csv("output.csv", index=False, header=True)
spark.stop()


