"""
reddit scraper v3 for moviepulse
fixed: v2 was pulling random non-movie comments (like news about cities)
now checks if posts/comments are actually about the movie before saving
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

headers = {"User-Agent": "moviepulse/1.0"}

# TODO: maybe switch to PRAW later for better rate limits
# reddit's json endpoint works fine for now tho


def get_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="MOVIE_ANALYTICS",
        schema="RAW"
    )


def load_movies():
    conn = get_conn()
    df = pd.read_sql("SELECT title, region FROM raw.movies_master", conn)
    conn.close()
    print(f"loaded {len(df)} movies from snowflake")
    return df


# subreddits per region
REGION_SUBS = {
    "Hollywood": ["movies", "flicks", "TrueFilm", "MovieSuggestions", "cinema", "Letterboxd"],
    "Bollywood": ["bollywood", "india", "hindi", "BollywoodMusicVideos", "desi", "indians"],
    "Kollywood": ["kollywood", "tamil", "TamilNadu", "tamilmovies", "chennaicity"],
    "Tollywood": ["tollywood", "telugu", "AndhraPradesh", "Telangana", "telugumovies"],
    "Mollywood": ["MalayalamMovies", "kerala", "MalayalamMusic"],
}

# these subs are specifically about movies so if a post matches the title
# its probably about the movie and not something else
MOVIE_SUBS = {
    "movies", "flicks", "truefilm", "moviesuggestions", "cinema", "letterboxd",
    "bollywood", "bollywoodmusicvideos", "kollywood", "tamilmovies",
    "tollywood", "telugumovies", "malayalammovies", "malayalammusic"
}


def extract_rating(text):
    text = text.strip()

    # 8/10, 8.5/10
    m = re.search(r'(\d+(\.\d+)?)\s*/\s*10', text)
    if m:
        val = float(m.group(1))
        if 0 <= val <= 10:
            return round(val, 1)

    # 9 out of 10
    m = re.search(r'(\d+(\.\d+)?)\s*out\s*of\s*10', text, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        if 0 <= val <= 10:
            return round(val, 1)

    # 4/5
    m = re.search(r'(\d+(\.\d+)?)\s*/\s*5(?!\d)', text)
    if m:
        val = float(m.group(1))
        if 0 <= val <= 5:
            return round(val * 2, 1)

    # 4 stars
    m = re.search(r'(\d+(\.\d+)?)\s*stars?', text, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        if 0 <= val <= 5:
            return round(val * 2, 1)

    return None


# words that tell us the post/comment is about a movie
# googled the translations for hindi tamil telugu malayalam
MOVIE_WORDS = [
    "movie", "film", "review", "watched", "rating", "theater", "theatre",
    "cinema", "trailer", "ott", "netflix", "prime", "hotstar", "streaming",
    "box office", "director", "actor", "spoiler", "blockbuster", "flop",
    "फिल्म", "मूवी", "रिव्यू",          # hindi
    "படம்", "திரைப்படம்", "விமர்சனம்",    # tamil
    "సినిమా", "రివ్యూ",                   # telugu
    "സിനിമ", "റിവ്യൂ",                    # malayalam
]

# for checking if comments are about movies (not cities, news, etc)
# same idea - googled common movie discussion words in regional languages
COMMENT_SIGNALS = [
    "movie", "film", "watch", "scene", "acting", "director", "plot",
    "story", "character", "ending", "screenplay", "cinematography",
    "soundtrack", "bgm", "review", "rating", "theater", "theatre",
    "netflix", "prime", "hotstar", "/10", "masterpiece", "boring",
    "overrated", "underrated", "must watch", "worth watching", "blockbuster",
    # hindi
    "फिल्म", "मूवी", "रिव्यू", "एक्टिंग", "डायरेक्टर", "कहानी",
    # tamil
    "படம்", "நடிப்பு", "கதை", "இயக்குனர்", "விமர்சனம்",
    # telugu
    "సినిమా", "నటన", "కథ", "దర్శకుడు", "రివ్యూ",
    # malayalam
    "സിനിമ", "അഭിനയം", "കഥ", "സംവിധായകൻ", "റിവ്യൂ",
]


def clean_title(title):
    """
    generate search-friendly versions of a movie title
    e.g. 'Guardians of the Galaxy Vol. 3' -> ['Guardians of the Galaxy Vol 3', 'Guardians of the Galaxy']
    'Indiana Jones and the Dial of Destiny' -> also searches 'Indiana Jones'
    'Mission: Impossible - Dead Reckoning Part One' -> also 'Mission Impossible'
    """
    queries = set()

    # original title
    queries.add(title)

    # remove punctuation (Vol. -> Vol, etc)
    cleaned = re.sub(r'[^\w\s]', '', title).strip()
    cleaned = re.sub(r'\s+', ' ', cleaned)
    queries.add(cleaned)

    # drop common suffixes like Vol 3, Part 2, Chapter 1
    short = re.sub(r'\b(vol|volume|part|chapter)\s*\d+\s*$', '', cleaned, flags=re.IGNORECASE).strip()
    if len(short) > 3 and short != cleaned:
        queries.add(short)

    # drop sequel numbers at the end: "Deadpool 3" -> "Deadpool"
    no_num = re.sub(r'\s+\d+\s*$', '', cleaned).strip()
    if len(no_num) > 3 and no_num != cleaned:
        queries.add(no_num)

    # drop year in parens: "Dune (2021)" -> "Dune"
    no_year = re.sub(r'\s*\(\d{4}\)\s*', '', title).strip()
    if no_year != title:
        queries.add(no_year)

    # split on subtitle separators (: - –)
    # "Indiana Jones and the Dial of Destiny" doesnt have one
    # but "Mission: Impossible - Dead Reckoning" -> "Mission Impossible"
    for sep in [':', ' - ', ' – ', ' — ']:
        if sep in title:
            main_part = title.split(sep)[0].strip()
            main_clean = re.sub(r'[^\w\s]', '', main_part).strip()
            if len(main_clean) > 3:
                queries.add(main_clean)

    # for long titles (5+ words), also try just the first 3 significant words
    # "Indiana Jones and the Dial of Destiny" -> "Indiana Jones"
    # helps because reddit search works better with shorter queries
    words = cleaned.split()
    skip_words = {"the", "and", "of", "in", "a", "an", "to", "for", "is", "at", "on"}
    significant = [w for w in words if w.lower() not in skip_words]
    if len(significant) >= 3:
        short_name = " ".join(significant[:2])
        if len(short_name) > 5:
            queries.add(short_name)

    return list(queries)


def is_movie_post(post_title, subreddit, movie_title):
    """
    v2 had a big problem - searching 'Agra' in r/india pulled up news
    about the city, not the movie. this checks if the post is actually
    about a movie before we grab comments from it.

    v3.1: also rejects general discussion threads on movie subs
    v3.2: on movie subs, trust reddit search more — people dont always
    type the full title (GOTG3 instead of Guardians of the Galaxy Vol 3)
    """
    title_lower = post_title.lower()

    # check if ANY version of the movie name appears in the post title
    title_variants = clean_title(movie_title)
    title_match = any(v.lower() in title_lower for v in title_variants)

    # also check for first significant word of the title
    # so "Guardians" matches "Just saw Guardians 3"
    title_words = [w for w in movie_title.split() if len(w) > 4]
    partial_match = any(w.lower() in title_lower for w in title_words[:2])

    # reject general discussion/list threads
    # ONLY reject posts that are clearly asking for lists or recommendations
    # NOT posts like "Deadpool is my favorite marvel movie" or "Animal is overrated"
    # the key difference: list threads START with question words or have list formats
    general_thread_patterns = [
        "what are the", "what is the", "which movie", "which film",
        "best of 20", "worst of 20",  # "best of 2024" etc
        "top 10", "top 5", "top 20",
        "recommend me", "recommendations for", "suggest me",
        "looking for movies", "looking for a movie",
        "what should i watch",
        "weekly discussion", "monthly discussion", "megathread",
        "tier list", "rank these", "ranking the",
    ]
    is_general = any(w in title_lower for w in general_thread_patterns)

    # for movie subs: reddit search already found it as relevant
    # trust it more, just reject obvious list threads
    if subreddit.lower() in MOVIE_SUBS:
        if is_general and not title_match:
            # its a list thread AND the movie name isnt even in the title = skip
            return False
        # full or partial title match = good
        if title_match or partial_match:
            return True
        # no match at all but reddit returned it — skip to be safe
        return False

    # for general subs like r/india: need full title match + movie keyword
    if not title_match:
        return False
    if is_general:
        return False

    for w in MOVIE_WORDS:
        if w in title_lower:
            return True

    if re.search(r'\d+\s*/\s*10', title_lower):
        return True

    return False


def is_about_movie(text, movie_title, is_movie_sub=False):
    """check if a comment is actually discussing the movie"""
    text_lower = text.lower()

    # mentions the movie by name = definitely relevant
    if movie_title.lower() in text_lower:
        return True

    # has a rating = probably about the movie
    if extract_rating(text) is not None:
        return True

    # on movie-specific subs, if the post already passed our filter,
    # most comments will be about the movie. only drop very short
    # low-effort comments that could be about anything
    if is_movie_sub:
        # skip super short stuff like "lol" "ok" "same"
        if len(text.strip()) < 20:
            return False
        # skip if its clearly about something else entirely
        offtopic = ["cricket", "election", "modi", "trump", "stock market"]
        if any(w in text_lower for w in offtopic):
            return False
        return True

    # for general subs, need movie-discussion words
    for s in COMMENT_SIGNALS:
        if s in text_lower:
            return True

    return False


def scrape_movie(movie_title, subreddit):
    """scrape comments for one movie from one subreddit"""
    rows = []

    # generate multiple search queries from the title
    # so "Guardians of the Galaxy Vol. 3" also searches "Guardians of the Galaxy"
    title_variants = clean_title(movie_title)

    all_queries = []
    for t in title_variants:
        if subreddit.lower() in MOVIE_SUBS:
            all_queries.append(t)
        else:
            # for general subs add "movie" to avoid random results
            # learned this the hard way with Agra lol
            all_queries.append(f"{t} movie")
            all_queries.append(f"{t} review")

    # dedupe queries (case insensitive)
    seen_q = set()
    queries = []
    for q in all_queries:
        if q.lower() not in seen_q:
            seen_q.add(q.lower())
            queries.append(q)

    seen_posts = set()

    for query in queries:
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
        params = {"q": query, "sort": "relevance", "limit": 10, "restrict_sr": 1}

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            
            # retry once if rate limited
            if resp.status_code == 429:
                time.sleep(5)
                resp = requests.get(url, params=params, headers=headers, timeout=20)
            
            if resp.status_code != 200:
                continue
            
            try:
                data = resp.json()
            except:
                # empty response = rate limited, wait and skip
                time.sleep(3)
                continue
            
            posts = data["data"]["children"]

            for post in posts:
                p = post["data"]

                if p["id"] in seen_posts:
                    continue
                seen_posts.add(p["id"])

                # NEW in v3: check if post is actually about the movie
                if not is_movie_post(p.get("title", ""), subreddit, movie_title):
                    continue

                # grab comments from this post
                try:
                    comm_url = f"https://www.reddit.com/r/{subreddit}/comments/{p['id']}.json"
                    comm_resp = requests.get(comm_url, headers=headers, timeout=20)
                    
                    if comm_resp.status_code == 429:
                        time.sleep(5)
                        comm_resp = requests.get(comm_url, headers=headers, timeout=20)
                    
                    cdata = comm_resp.json()
                    comments = cdata[1]["data"]["children"]

                    for c in comments[:100]:
                        if c["kind"] != "t1":
                            continue
                        body = c["data"].get("body", "")
                        if len(body) < 10 or body in ("[deleted]", "[removed]"):
                            continue

                        # NEW in v3: check if comment is about the movie
                        if not is_about_movie(body, movie_title,
                                              is_movie_sub=subreddit.lower() in MOVIE_SUBS):
                            continue

                        rows.append({
                            "movie_name": movie_title,
                            "subreddit": subreddit,
                            "post_title": p.get("title", "")[:300],
                            "comment_text": body[:1000],
                            "upvotes": c["data"].get("score", 0),
                            "comment_timestamp": str(
                                datetime.utcfromtimestamp(c["data"]["created_utc"])
                            ),
                            "extracted_rating": extract_rating(body)
                        })
                except:
                    pass  # sometimes reddit just 429s us, wait a bit
                
                time.sleep(1)  # sleep between comment fetches too

            time.sleep(2)  # longer sleep between searches

        except Exception as e:
            print(f"  error [{subreddit}] {query[:30]}: {e}")

    return rows


# ─── main ─────────────────────────────────────────────

def run_all():
    movies_df = load_movies()
    all_rows = []
    total = len(movies_df)
    skipped = 0

    print(f"\nscraping {total} movies across {len(REGION_SUBS)} regions...\n")

    for i, row in movies_df.iterrows():
        title = row["TITLE"]
        region = row["REGION"]
        subs = REGION_SUBS.get(region, ["movies"])

        before = len(all_rows)
        movie_comments = []
        for sub in subs:
            rows = scrape_movie(title, sub)
            movie_comments.extend(rows)
            # cap at 200 per movie — dont waste rate limit on one movie
            if len(movie_comments) >= 200:
                movie_comments = movie_comments[:200]
                break
        all_rows.extend(movie_comments)

        got = len(all_rows) - before
        if got == 0:
            skipped += 1

        if i % 10 == 0:
            print(f"  {i}/{total} | {title[:40]} | +{got} comments | total: {len(all_rows)}")

        # checkpoint in case it crashes (it will at some point lol)
        if i % 50 == 0 and len(all_rows) > 0:
            pd.DataFrame(all_rows).to_csv("data/reddit_checkpoint.csv", index=False)

        time.sleep(3)  # give reddit time to cool down between movies

    print(f"\n--- done scraping ---")
    print(f"movies with 0 comments: {skipped}/{total}")

    # dedupe
    df = pd.DataFrame(all_rows)
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["movie_name", "comment_text"])
    df = df.reset_index(drop=True)
    print(f"removed {before_dedup - len(df)} duplicate comments")

    rated = df["extracted_rating"].notna().sum()
    print(f"\nfinal: {len(df)} comments for {df['movie_name'].nunique()} movies")
    print(f"comments with explicit ratings: {rated} ({round(rated/len(df)*100, 1)}%)")

    # save backup first in case snowflake fails
    df.to_csv("data/reddit_raw_v3_backup.csv", index=False)
    print("saved backup to data/reddit_raw_v3_backup.csv")

    # push to snowflake
    print("\npushing to snowflake...")
    push_df = df.copy()
    push_df.columns = [c.upper() for c in push_df.columns]
    push_df["UPVOTES"] = push_df["UPVOTES"].fillna(0).astype(int)

    conn = get_conn()
    write_pandas(conn, push_df, "REDDIT_COMMENTS_V3",
                 database="MOVIE_ANALYTICS", schema="RAW")
    conn.close()
    print(f"done! pushed {len(push_df)} comments to snowflake")


if __name__ == "__main__":
    run_all()
