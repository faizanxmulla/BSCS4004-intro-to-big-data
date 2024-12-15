from kafka import KafkaProducer
import pandas as pd
import time
import json


file_path = "gs://ibd-oppe-bucket-2/cleaned_stock_data.csv"
topic="stock_data"

batch_size = 10

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


try:
    df = pd.read_csv(file_path)

    records_processed = 0
    
    total_records = len(df)
    print(f"Preparing to send {total_records} records to Kafka...")

    while records_processed < total_records:
        batch_end = min(records_processed + batch_size, total_records)
        batch = df.iloc[records_processed:batch_end].to_dict("records")

        print(
            f"\nSending batch of {len(batch)} records (records {records_processed + 1} to {batch_end})..."
        )
        producer.send(topic, batch)

        records_processed += len(batch)
        print(f"Total records processed so far: {records_processed}/{total_records}")

        if records_processed < total_records:
            print("Sleeping for 10 seconds")
            time.sleep(10)

    print("\nAll records processed and sent to Kafka successfully!")

except Exception as e:
    print("An error occurred:", e)

