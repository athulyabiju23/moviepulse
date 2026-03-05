import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection(schema="raw"):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=schema
    )
    return conn

def push_to_snowflake(df, table, schema="raw"):
    conn = get_connection(schema)
    cursor = conn.cursor()
    success, failed = 0, 0

    for _, row in df.iterrows():
        try:
            cursor.execute(f"""
                INSERT INTO {schema}.{table}
                ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(row))})
            """, tuple(row))
            success += 1
        except Exception as e:
            failed += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {success} rows | ❌ Failed: {failed}")

def read_from_snowflake(query, schema="raw"):
    conn = get_connection(schema)
    df = pd.read_sql(query, conn)
    conn.close()
    return df
