import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_sample_data(num_rows=1000):
    # Generate sample data
    data = {
        "timestamp": [
            (datetime.now() - timedelta(days=x)).strftime("%Y-%m-%d %H:%M:%S")
            for x in range(num_rows)
        ],
        "user_id": np.random.randint(1, 1000, num_rows),
        "product_id": np.random.randint(1, 100, num_rows),
        "quantity": np.random.randint(1, 10, num_rows),
        "price": np.random.uniform(10.0, 1000.0, num_rows).round(2),
    }

    df = pd.DataFrame(data)
    # Save to CSV
    df.to_csv("sales_data.csv", index=False)
    print("Generated sample data file: sales_data.csv")


if __name__ == "__main__":
    generate_sample_data(1000)
