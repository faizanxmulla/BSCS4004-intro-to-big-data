from kafka import KafkaProducer
import pandas as pd
import json
import time
from typing import List


class SalesDataProducer:
    def __init__(self, bootstrap_servers: str, topic_name: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        self.topic = topic_name

    def send_batch(self, records: List[dict]):
        for record in records:
            self.producer.send(self.topic, value=record)
        self.producer.flush()

    def process_file(self, file_path: str, batch_size: int = 10):
        df = pd.read_csv(file_path)
        records_processed = 0
        total_records = min(1000, len(df))  # Process maximum 1000 records

        while records_processed < total_records:
            batch_end = min(records_processed + batch_size, total_records)
            batch = df.iloc[records_processed:batch_end].to_dict("records")

            print(f"Sending batch of {len(batch)} records...")
            self.send_batch(batch)

            records_processed += len(batch)
            print(f"Total records processed: {records_processed}/{total_records}")

            if records_processed < total_records:
                print("Sleeping for 10 seconds...")
                time.sleep(10)

        self.producer.close()


if __name__ == "__main__":
    producer = SalesDataProducer(
        bootstrap_servers="localhost:9092", topic_name="sales_data"
    )
    producer.process_file("gs://iitm-ibd-ga7/sales_data.csv")