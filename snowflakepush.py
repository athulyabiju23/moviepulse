import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

load_dotenv()

df = pd.read_csv("data/reddit_raw_v3_backup.csv")
df.columns = [c.upper() for c in df.columns]
df["UPVOTES"] = df["UPVOTES"].fillna(0).astype(int)

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="COMPUTE_WH",
    database="MOVIE_ANALYTICS",
    schema="RAW"
)

write_pandas(conn, df, "REDDIT_COMMENTS_V3",
             database="MOVIE_ANALYTICS", schema="RAW")
conn.close()
print(f"done! pushed {len(df)} rows")