import requests
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time

# ---- Snowflake connection ----
def get_conn():
    return snowflake.connector.connect(
        user="ATHULYA2303",
        password="Athulyabiju@2303",
        account="gsc07824.us-east-1",
        warehouse="COMPUTE_WH",
        database="MOVIE_ANALYTICS",
        schema="RAW"
    )

# ---- Load real movies from Snowflake ----
def load_movies():
    conn = get_conn()
    df = pd.read_sql("SELECT title, region FROM raw.movies_master", conn)
    conn.close()
    print(f"✅ Loaded {len(df)} movies from Snowflake")
    return df

# ---- Map region to subreddits ----
REGION_SUBREDDITS = {
    "Hollywood":  ["movies", "flicks"],
    "Bollywood":  ["bollywood", "hindi"],
    "Kollywood":  ["kollywood", "tamil"],
    "Tollywood":  ["tollywood", "telugu"],
    "Mollywood":  ["MalayalamMovies", "kerala"],
}

headers = {"User-Agent": "moviepulse/1.0"}

# ---- Search Reddit for a specific movie ----
def search_reddit(movie_title, subreddit):
    rows = []
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {
        "q":      movie_title,
        "sort":   "relevance",
        "limit":  5,
        "restrict_sr": 1
    }

    try:
        posts = requests.get(
            url, params=params, headers=headers
        ).json()

        for post in posts["data"]["children"]:
            p = post["data"]

            # Only process posts that mention the movie title
            if movie_title.lower() not in p["title"].lower():
                continue

            comments_url = f"https://www.reddit.com/r/{subreddit}/comments/{p['id']}.json"
            try:
                comments_data = requests.get(
                    comments_url, headers=headers
                ).json()
                comments = comments_data[1]["data"]["children"]

                for c in comments[:15]:
                    if c["kind"] != "t1":
                        continue
                    body = c["data"].get("body", "")
                    if len(body) < 10 or body == "[deleted]":
                        continue

                    rows.append({
                        "movie_name":         movie_title,
                        "subreddit":          subreddit,
                        "comment_text":       body[:1000],
                        "upvotes":            c["data"].get("score", 0),
                        "comment_timestamp":  datetime.utcfromtimestamp(
                                                c["data"]["created_utc"]
                                              ),
                        "extracted_rating":   None
                    })
            except:
                continue

    except Exception as e:
        print(f"    ❌ Error: {e}")

    return rows

# ---- Run for all movies ----
def run_all():
    movies_df = load_movies()

    # Clear old bad data first
    conn = get_conn()
    conn.cursor().execute("TRUNCATE TABLE raw.reddit_comments")
    conn.close()
    print("🗑️  Cleared old Reddit comments")

    all_rows = []
    total = len(movies_df)

    for i, row in movies_df.iterrows():
        title  = row["TITLE"]
        region = row["REGION"]
        subs   = REGION_SUBREDDITS.get(region, ["movies"])

        for sub in subs:
            rows = search_reddit(title, sub)
            all_rows.extend(rows)

        if i % 10 == 0:
            print(f"  → {i}/{total} | {title} | comments so far: {len(all_rows)}")

        time.sleep(0.5)  # be nice to Reddit

    # Save backup
    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["movie_name", "comment_text"])
    df.to_csv("data/reddit_raw_backup.csv", index=False)
    print(f"\n✅ Total: {len(df)} comments for {df['movie_name'].nunique()} movies")

    # Push to Snowflake
    print("❄️  Pushing to Snowflake...")
    df.columns = [c.upper() for c in df.columns]
    df["COMMENT_TIMESTAMP"] = df["COMMENT_TIMESTAMP"].astype(str)
    df["UPVOTES"] = df["UPVOTES"].fillna(0).astype(int)

    conn = get_conn()
    write_pandas(conn, df, "REDDIT_COMMENTS",
                 database="MOVIE_ANALYTICS", schema="RAW")
    conn.close()
    print(f"✅ Pushed {len(df)} comments to Snowflake!")

if __name__ == "__main__":
    run_all()