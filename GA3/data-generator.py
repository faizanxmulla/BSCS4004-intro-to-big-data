import csv
import random
from datetime import timedelta, datetime


def generate_random_timestamp(base_date=datetime(2024, 10, 15)):
    return base_date + timedelta(
        days=random.randint(0, 365),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


data = {f"user_{i + 1}": generate_random_timestamp() for i in range(0, 50)}

output_file = "data.csv"

with open(output_file, "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["id", "timestamp", "date"])
    writer.writerows(
        (user_id, ts.strftime("%Y-%m-%d %H:%M:%S"), ts.strftime("%Y-%m-%d"))
        for user_id, ts in data.items()
    )