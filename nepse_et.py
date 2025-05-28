import pandas as pd
from datetime import datetime, timedelta

RAW_PATH = "local_datalake/raw/merged_stocks.csv"
PROCESSED_PATH = "local_datalake/processed/cleaned_stocks.csv"

#Extraction
df = pd.read_csv(RAW_PATH)

#Cleaning And Transformation

df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

df = df.dropna(subset=['Date'])

df = df.drop_duplicates(subset=['Date'], keep='last')

# removing stocks which haven't been traded for the last 30 days

df['Date'] = pd.to_datetime(df['Date'])

latest_date = df['Date'].max()

recent_df = df[df['Date'] >= (latest_date - timedelta(days=30))]

inactive_stock = recent_df.loc[:,recent_df.columns != 'Date'].isna().all()

df = df.drop(columns= inactive_stock[inactive_stock].index.tolist())

#converting all the columns except date to numeric

for col in df.columns:
    if col != 'Date':
        df[col] = pd.to_numeric(df[col],errors='coerce')
        
# saving cleaned file to csv in processed 

df.to_csv(PROCESSED_PATH, index= False)

print("Clean file saved to processed data lake")