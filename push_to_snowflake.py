import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

load_dotenv()

# Load the scraped data
df = pd.read_csv("data/reddit_raw_backup.csv")
print(f"📦 Loaded {len(df)} rows from CSV")

# Fix column names to match Snowflake (uppercase)
df.columns = [c.upper() for c in df.columns]

# Fix null values
df["EXTRACTED_RATING"] = df["EXTRACTED_RATING"].where(pd.notna(df["EXTRACTED_RATING"]), None)
df["UPVOTES"] = df["UPVOTES"].fillna(0).astype(int)
df["COMMENT_TEXT"] = df["COMMENT_TEXT"].astype(str).str[:1000]
df["MOVIE_NAME"] = df["MOVIE_NAME"].astype(str).str[:200]
df["COMMENT_TIMESTAMP"] = df["COMMENT_TIMESTAMP"].astype(str)

print("🔌 Connecting to Snowflake...")

conn = snowflake.connector.connect(
    user="ATHULYA2303",
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account="gsc07824.us-east-1",
    warehouse="COMPUTE_WH",
    database="MOVIE_ANALYTICS",
    schema="RAW"
)

print("⬆️  Uploading all 936 rows at once...")

success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    table_name="REDDIT_COMMENTS",
    database="MOVIE_ANALYTICS",
    schema="RAW"
)

print(f"✅ Done! Inserted {nrows} rows in {nchunks} chunks")
conn.close()