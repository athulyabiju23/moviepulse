"""
Reddit Comment Scraper for MoviePulse
Scrapes movie discussions from Reddit across multiple regional subreddits
and extracts user ratings from comments.
"""

import requests
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os
import time
import re

load_dotenv()


# ── Snowflake connection ──────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="MOVIE_ANALYTICS",
        schema="RAW"
    )


# ── Load movies from Snowflake ───────────────────────────────────
def load_movies():
    conn = get_conn()
    df = pd.read_sql("SELECT title, region FROM raw.movies_master", conn)
    conn.close()
    print(f"Loaded {len(df)} movies from Snowflake")
    return df


# ── Subreddits per region ─────────────────────────────────────────
# each region has a list of subreddits where people discuss those movies
REGION_SUBREDDITS = {
    "Hollywood": [
        "movies", "flicks", "TrueFilm",
        "MovieSuggestions", "cinema", "Letterboxd"
    ],
    "Bollywood": [
        "bollywood", "india", "hindi",
        "BollywoodMusicVideos", "desi", "indians"
    ],
    "Kollywood": [
        "kollywood", "tamil", "TamilNadu",
        "tamilmovies", "chennaicity"
    ],
    "Tollywood": [
        "tollywood", "telugu", "AndhraPradesh",
        "Telangana", "telugumovies"
    ],
    "Mollywood": [
        "MalayalamMovies", "kerala",
        "MalayalamMusic"
    ],
}

headers = {"User-Agent": "moviepulse/1.0"}


# ── Extract ratings from comment text ─────────────────────────────
# people rate movies in different ways — we try to catch all of them
def extract_rating(text):
    text = text.strip()

    # 8/10, 8.5/10
    match = re.search(r'(\d+(\.\d+)?)\s*/\s*10', text, re.IGNORECASE)
    if match:
        val = float(match.group(1))
        if 0 <= val <= 10:
            return round(val, 1)

    # 9 out of 10
    match = re.search(r'(\d+(\.\d+)?)\s*out\s*of\s*10', text, re.IGNORECASE)
    if match:
        val = float(match.group(1))
        if 0 <= val <= 10:
            return round(val, 1)

    # 4/5 → convert to /10
    match = re.search(r'(\d+(\.\d+)?)\s*/\s*5(?!\d)', text, re.IGNORECASE)
    if match:
        val = float(match.group(1))
        if 0 <= val <= 5:
            return round(val * 2, 1)

    # 4 stars → convert to /10
    match = re.search(r'(\d+(\.\d+)?)\s*stars?', text, re.IGNORECASE)
    if match:
        val = float(match.group(1))
        if 0 <= val <= 5:
            return round(val * 2, 1)

    # 3/4 → convert to /10
    match = re.search(r'(\d+(\.\d+)?)\s*/\s*4(?!\d)', text, re.IGNORECASE)
    if match:
        val = float(match.group(1))
        if 0 <= val <= 4:
            return round((val / 4) * 10, 1)

    return None


# ── Search Reddit for a movie ─────────────────────────────────────
def search_reddit(movie_title, subreddit):
    rows = []

    # try different cases to catch more results
    search_queries = [
        movie_title,
        movie_title.lower(),
        movie_title.upper(),
        movie_title.title(),
        movie_title.replace(" ", ""),
    ]

    # remove duplicates
    seen = set()
    unique_queries = []
    for q in search_queries:
        if q.lower() not in seen:
            seen.add(q.lower())
            unique_queries.append(q)

    seen_post_ids = set()

    for query in unique_queries:
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
        params = {
            "q": query,
            "sort": "relevance",
            "limit": 10,
            "restrict_sr": 1
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=20)

            if response.status_code != 200:
                continue

            posts = response.json()

            for post in posts["data"]["children"]:
                p = post["data"]

                if p["id"] in seen_post_ids:
                    continue
                seen_post_ids.add(p["id"])

                # get comments for this post
                comments_url = f"https://www.reddit.com/r/{subreddit}/comments/{p['id']}.json"

                try:
                    comments_data = requests.get(
                        comments_url, headers=headers, timeout=20
                    ).json()

                    comments = comments_data[1]["data"]["children"]

                    for c in comments[:20]:  # top 20 comments per post
                        if c["kind"] != "t1":
                            continue

                        body = c["data"].get("body", "")
                        if len(body) < 10 or body in ("[deleted]", "[removed]"):
                            continue

                        rows.append({
                            "movie_name": movie_title,
                            "subreddit": subreddit,
                            "comment_text": body[:1000],
                            "upvotes": c["data"].get("score", 0),
                            "comment_timestamp": str(
                                datetime.utcfromtimestamp(c["data"]["created_utc"])
                            ),
                            "extracted_rating": extract_rating(body)
                        })

                except Exception:
                    continue

            time.sleep(1)

        except Exception as e:
            print(f"  Error [{subreddit}] '{query[:30]}': {e}")
            continue

    return rows


# ── Main function ─────────────────────────────────────────────────
def run_all():
    movies_df = load_movies()
    all_rows = []
    total = len(movies_df)

    for i, row in movies_df.iterrows():
        title = row["TITLE"]
        region = row["REGION"]
        subs = REGION_SUBREDDITS.get(region, ["movies"])

        for sub in subs:
            rows = search_reddit(title, sub)
            all_rows.extend(rows)

        # progress update every 10 movies
        if i % 10 == 0:
            print(f"  {i}/{total} | {title[:40]} | comments so far: {len(all_rows)}")

        # checkpoint save every 50 movies
        if i % 50 == 0 and len(all_rows) > 0:
            pd.DataFrame(all_rows).to_csv("data/reddit_checkpoint.csv", index=False)

        time.sleep(1)

    # deduplicate
    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["movie_name", "comment_text"])
    df = df.reset_index(drop=True)

    # stats
    rated = df["extracted_rating"].notna().sum()
    print(f"\nTotal: {len(df)} comments for {df['movie_name'].nunique()} movies")
    print(f"Comments with explicit ratings: {rated} ({round(rated/len(df)*100, 1)}%)")

    # save locally
    df.to_csv("data/reddit_raw_v2_backup.csv", index=False)
    print("Saved to data/reddit_raw_v2_backup.csv")

    # push to snowflake
    print("Pushing to Snowflake...")
    push_df = df.copy()
    push_df.columns = [c.upper() for c in push_df.columns]
    push_df["UPVOTES"] = push_df["UPVOTES"].fillna(0).astype(int)

    conn = get_conn()
    write_pandas(
        conn, push_df, "REDDIT_COMMENTS_V2",
        database="MOVIE_ANALYTICS", schema="RAW"
    )
    conn.close()
    print(f"Pushed {len(push_df)} comments to Snowflake!")


if __name__ == "__main__":
    run_all()