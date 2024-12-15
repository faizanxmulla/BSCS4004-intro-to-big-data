import pandas as pd
import os

input_folder = "NSE_Stocks_Data-20241215T082805Z-001/NSE_Stocks_Data/"

stock_data_corpus = []


for file in os.listdir(input_folder):
    if file.endswith(".csv"):
        company_name = file.replace(".csv", "")

        df = pd.read_csv(os.path.join(input_folder, file))
        df['company_name'] = company_name

        stock_data_corpus.append(df)

stock_data_df = pd.concat(stock_data_corpus)
stock_data_df.to_csv("all_companies_stock_data_combined.csv", index=False)


# combined CSV file is very big --> > 3.05 GB
