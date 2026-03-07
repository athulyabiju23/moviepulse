# import requests
# import re
# import time

# headers = {"User-Agent": "moviepulse/1.0"}

# movie = "Guardians of the Galaxy Vol. 3"
# subreddit = "movies"

# def clean_title(title):
#     queries = set()
#     queries.add(title)
#     cleaned = re.sub(r'[^\w\s]', '', title).strip()
#     cleaned = re.sub(r'\s+', ' ', cleaned)
#     queries.add(cleaned)
#     short = re.sub(r'\b(vol|volume|part|chapter)\s*\d+\s*$', '', cleaned, flags=re.IGNORECASE).strip()
#     if len(short) > 3 and short != cleaned:
#         queries.add(short)
#     no_num = re.sub(r'\s+\d+\s*$', '', cleaned).strip()
#     if len(no_num) > 3 and no_num != cleaned:
#         queries.add(no_num)
#     for sep in [':', ' - ', ' – ', ' — ']:
#         if sep in title:
#             main_part = title.split(sep)[0].strip()
#             main_clean = re.sub(r'[^\w\s]', '', main_part).strip()
#             if len(main_clean) > 3:
#                 queries.add(main_clean)
#     words = cleaned.split()
#     skip_words = {"the", "and", "of", "in", "a", "an", "to", "for", "is", "at", "on"}
#     significant = [w for w in words if w.lower() not in skip_words]
#     if len(significant) >= 3:
#         short_name = " ".join(significant[:2])
#         if len(short_name) > 5:
#             queries.add(short_name)
#     return list(queries)

# variants = clean_title(movie)
# print(f"Title variants: {variants}\n")

# for query in variants:
#     url = f"https://www.reddit.com/r/{subreddit}/search.json"
#     params = {"q": query, "sort": "relevance", "limit": 5, "restrict_sr": 1}
#     try:
#         resp = requests.get(url, params=params, headers=headers, timeout=20)
#         if resp.status_code != 200:
#             print(f"Query: '{query}' -> status {resp.status_code}")
#             time.sleep(3)
#             continue
#         data = resp.json()
#         posts = data["data"]["children"]
#         print(f"Query: '{query}' -> {len(posts)} posts")
#         for p in posts[:3]:
#             print(f"  {p['data']['title'][:80]}")
#     except:
#         print(f"Query: '{query}' -> FAILED (rate limited)")
#     print()
#     time.sleep(3)

# import pandas as pd
# import time
# from scraper_3 import scrape_movie, load_movies, REGION_SUBS, get_conn
# from snowflake.connector.pandas_tools import write_pandas

# # load what we already have
# df = pd.read_csv("data/reddit_raw_v3_backup.csv")
# scraped = set(df["movie_name"].unique())
# print(f"Already have: {len(scraped)} movies")

# # find missing ones
# movies_df = load_movies()
# zero_movies = movies_df[~movies_df["TITLE"].isin(scraped)]
# print(f"Retrying: {len(zero_movies)} movies\n")

# retry_rows = []
# for i, (_, row) in enumerate(zero_movies.iterrows()):
#     title = row["TITLE"]
#     region = row["REGION"]
#     subs = REGION_SUBS.get(region, ["movies"])

#     movie_comments = []
#     for sub in subs:
#         rows = scrape_movie(title, sub)
#         movie_comments.extend(rows)
#         if len(movie_comments) >= 200:
#             movie_comments = movie_comments[:200]
#             break
#     retry_rows.extend(movie_comments)

#     if len(movie_comments) > 0:
#         print(f"  ✓ {title} | +{len(movie_comments)}")

#     if i % 20 == 0:
#         print(f"  {i}/{len(zero_movies)} retried | +{len(retry_rows)} comments so far")

#     time.sleep(3)

# # merge with existing data
# if retry_rows:
#     retry_df = pd.DataFrame(retry_rows)
#     df = pd.concat([df, retry_df]).drop_duplicates(subset=["movie_name", "comment_text"])
#     df = df.reset_index(drop=True)
#     print(f"\nAfter retry: {len(df)} comments for {df['movie_name'].nunique()} movies")
#     df.to_csv("data/reddit_raw_v3_backup.csv", index=False)
    
#     # push to snowflake
#     push_df = df.copy()
#     push_df.columns = [c.upper() for c in push_df.columns]
#     push_df["UPVOTES"] = push_df["UPVOTES"].fillna(0).astype(int)
#     conn = get_conn()
#     write_pandas(conn, push_df, "REDDIT_COMMENTS_V3",
#                  database="MOVIE_ANALYTICS", schema="RAW",
#                  overwrite=True)
#     conn.close()
#     print("Pushed to snowflake!")
# else:
#     print("No new comments found in retry")


import pandas as pd

df = pd.read_csv("data/reddit_sentiment_multilingual.csv")
comp = pd.read_csv("data/platform_comparison.csv")

# movies with less than 5 chars in the title - these are the problematic ones
short_title = comp[comp["movie_name"].str.len() <= 5]
print(f"Movies with short titles (<=5 chars): {len(short_title)}")
print(short_title["movie_name"].tolist())

print(f"\nTotal movies in comparison: {len(comp)}")
print(f"Movies with 20+ comments: {len(comp[comp['total_comments'] >= 20])}")