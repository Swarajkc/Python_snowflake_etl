import pandas as pd
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

load_dotenv()

user = os.getenv("SNOWFLAKE_USER")
password = quote_plus(os.getenv("SNOWFLAKE_PASSWORD"))  # handle special chars like @
account = os.getenv("SNOWFLAKE_ACCOUNT")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

# Extract & Transform
df = pd.read_csv('details.csv')
df.dropna()
df['age'] = df['age'].astype(str)
df = df[df['graduation'].between(0,100)]

# Construct connection string
connection_string = (
    f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
)

# Connect and Load
engine = create_engine(connection_string)

# Drop table manually (only if it exists)
with engine.connect() as conn:
    conn.execute(text(f'DROP TABLE IF EXISTS {schema}."PEOPLE"'))

df.to_sql('PEOPLE', con=engine, schema=schema, index=False)

print("✅ ETL completed and data loaded into Snowflake.")
