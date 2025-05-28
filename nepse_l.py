import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import pandas as pd

load_dotenv()

user = os.getenv("SNOWFLAKE_USER")
password = quote_plus(os.getenv("SNOWFLAKE_PASSWORD"))
account = os.getenv("SNOWFLAKE_ACCOUNT")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

# constructing connection string 

connection_string = (
    f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
)

# connect and load
engine = create_engine(connection_string)

#dropping table if it exists

with engine.connect() as conn:
    conn.execute(text(f'DROP TABLE IF EXISTS {schema}."NEPSE_ETL"'))
    

#extracting the processed data 
df = pd.read_csv("local_datalake/processed/cleaned_stocks.csv")
df.to_sql('NEPSE_ETL',con=engine, schema=schema, index=False)

print("ETL successfully completed.")