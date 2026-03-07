# streamlit app for moviepulse
# run with: streamlit run app.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector

st.set_page_config(page_title="Reddit Movie Pulse", page_icon="🎬", layout="wide")

# ---- snowflake connection ----
@st.cache_data(ttl=600)
def load_data():
    """Load data from Snowflake. Falls back to local CSVs if connection fails."""
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"].get("schema", "ANALYTICS"),
        )
        cur = conn.cursor()

        cur.execute("SELECT * FROM PLATFORM_COMPARISON")
        comp = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        cur.execute("SELECT * FROM MOVIE_SCORES_V2")
        scores = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        cur.execute("SELECT * FROM SENTIMENT_MULTILINGUAL")
        comments = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        cur.close()
        conn.close()

        # snowflake returns UPPERCASE column names — lowercase them to match the rest of the app
        comp.columns = comp.columns.str.lower()
        scores.columns = scores.columns.str.lower()
        comments.columns = comments.columns.str.lower()

        comments["comment_timestamp"] = pd.to_datetime(comments["comment_timestamp"])
        return comp, scores, comments

    except Exception as e:
        st.warning(f"Snowflake connection failed ({e}), trying local CSVs...")
        comp = pd.read_csv("data/platform_comparison.csv")
        scores = pd.read_csv("data/reddit_movie_scores_v2.csv")
        comments = pd.read_csv("data/reddit_sentiment_multilingual.csv")
        comments["comment_timestamp"] = pd.to_datetime(comments["comment_timestamp"])
        return comp, scores, comments

try:
    comp, scores, comments = load_data()
except Exception as e:
    st.error(f"Can't load data: {e}")
    st.stop()

# ---- sidebar ----
st.sidebar.title("🎬 Movie Pulse")
st.sidebar.markdown("---")

regions = ["All"] + sorted(comp["region"].unique().tolist())
sel_region = st.sidebar.selectbox("Region", regions)

all_genres = set()
for g in comp["genre"].dropna():
    for x in g.split(", "):
        all_genres.add(x)
genres = ["All"] + sorted(all_genres)
sel_genre = st.sidebar.selectbox("Genre", genres)

min_comm = st.sidebar.slider("Min Comments", 3, 500, 20)

# filter
df = comp.copy()
if sel_region != "All":
    df = df[df["region"] == sel_region]
if sel_genre != "All":
    df = df[df["genre"].str.contains(sel_genre, na=False)]
df = df[df["total_comments"] >= min_comm]

st.sidebar.markdown("---")
st.sidebar.write(f"Showing **{len(df)}** movies")

# ---- header ----
st.title("Reddit Movie Pulse 🎬")
st.caption("comparing reddit opinions vs imdb and rotten tomatoes")

if len(df) == 0:
    st.warning("no movies match these filters, try changing them")
    st.stop()

# kpis
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Movies", len(df))
c2.metric("Reddit Avg", f"{df['reddit_score'].mean():.2f}")
c3.metric("IMDb Avg", f"{df['imdb_rating'].mean():.2f}")
c4.metric("Correlation", f"{df['reddit_score'].corr(df['imdb_rating']):.3f}")
c5.metric("RT Avg", f"{df['rt_score_10'].mean():.2f}")

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["Platform Comparison", "Movie Lookup", "Sentiment", "Predictions"])


# ===== TAB 1 =====
with tab1:
    left, right = st.columns([3, 2])

    with left:
        r = df["reddit_score"].corr(df["imdb_rating"])
        fig = px.scatter(
            df, x="imdb_rating", y="reddit_score",
            color="region", size="total_comments",
            hover_name="movie_name",
            title=f"Reddit vs IMDb (r={r:.3f})",
            opacity=0.7,
            color_discrete_map={"Hollywood":"#FF4500", "Bollywood":"#4169E1", "Kollywood":"#2ECC71"}
        )
        fig.add_trace(go.Scatter(x=[1,10], y=[1,10], mode="lines",
                      line=dict(dash="dash", color="gray"), name="perfect agreement"))
        fig.update_layout(xaxis_title="IMDb", yaxis_title="Reddit", height=420,
                         xaxis=dict(range=[2,9.5]), yaxis=dict(range=[3,8]))
        st.plotly_chart(fig, use_container_width=True)

    with right:
        reg = df.groupby("region").agg(
            reddit=("reddit_score","mean"), imdb=("imdb_rating","mean"),
            rt=("rt_score_10","mean")
        ).reset_index()

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(name="Reddit", x=reg["region"], y=reg["reddit"], marker_color="#FF4500"))
        fig2.add_trace(go.Bar(name="IMDb", x=reg["region"], y=reg["imdb"], marker_color="#F5C518"))
        fig2.add_trace(go.Bar(name="RT", x=reg["region"], y=reg["rt"], marker_color="#FA320A"))
        fig2.update_layout(title="Avg Ratings by Region", barmode="group",
                          yaxis=dict(range=[0,10]), height=420)
        st.plotly_chart(fig2, use_container_width=True)

    # genre comparison
    gdata = []
    for _, row in df.iterrows():
        if pd.notna(row["genre"]):
            for g in row["genre"].split(", "):
                gdata.append({"genre":g, "reddit":row["reddit_score"],
                             "imdb":row["imdb_rating"], "rt":row.get("rt_score_10", np.nan)})

    if len(gdata) > 0:
        gdf = pd.DataFrame(gdata)
        gavg = gdf.groupby("genre").agg(
            reddit=("reddit","mean"), imdb=("imdb","mean"),
            rt=("rt","mean"), n=("reddit","count")
        ).reset_index()
        gavg = gavg[gavg["n"] >= 5].sort_values("n", ascending=True)

        if len(gavg) > 0:
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(name="Reddit", y=gavg["genre"], x=gavg["reddit"], orientation="h", marker_color="#FF4500"))
            fig3.add_trace(go.Bar(name="IMDb", y=gavg["genre"], x=gavg["imdb"], orientation="h", marker_color="#F5C518"))
            fig3.add_trace(go.Bar(name="RT", y=gavg["genre"], x=gavg["rt"], orientation="h", marker_color="#FA320A"))
            fig3.update_layout(title="Ratings by Genre", barmode="group",
                              xaxis=dict(range=[0,10]), height=450)
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("not enough movies per genre to compare (need 5+ per genre)")
    else:
        st.info("no genre data available for current filters")

    # disagreements
    df["diff"] = df["reddit_score"] - df["imdb_rating"]
    left2, right2 = st.columns(2)

    with left2:
        st.subheader("Reddit rates HIGHER than IMDb")
        top10 = df.nlargest(10, "diff")[["movie_name","reddit_score","imdb_rating","diff"]]
        top10.columns = ["Movie","Reddit","IMDb","Gap"]
        st.dataframe(top10, hide_index=True, use_container_width=True)

    with right2:
        st.subheader("Reddit rates LOWER than IMDb")
        bot10 = df.nsmallest(10, "diff")[["movie_name","reddit_score","imdb_rating","diff"]]
        bot10.columns = ["Movie","Reddit","IMDb","Gap"]
        st.dataframe(bot10, hide_index=True, use_container_width=True)


# ===== TAB 2: movie lookup =====
with tab2:
    st.subheader("look up a movie")

    movies = sorted(df["movie_name"].unique().tolist())
    pick = st.selectbox("pick a movie", movies)

    if pick:
        m = df[df["movie_name"] == pick].iloc[0]
        m_comments = comments[comments["movie_name"] == pick].copy()

        # ── filter out irrelevant comments ──
        # comments scraped from general subs (r/india, r/kerala etc) often
        # match the movie title but are actually about cities, people, or news
        movie_signals = [
            "movie", "film", "watch", "scene", "acting", "actor", "actress",
            "director", "plot", "story", "character", "ending", "screenplay",
            "cinematography", "soundtrack", "ost", "bgm", "interval",
            "climax", "twist", "review", "rating", "imdb", "ott",
            "theater", "theatre", "netflix", "prime", "hotstar",
            "/10", "out of 10", "stars", "masterpiece", "boring",
            "overrated", "underrated", "must watch", "worth watching",
            "blockbuster", "flop", "hit", "paisa vasool",
        ]
        # subreddits where posts are almost certainly about movies
        movie_subs = {"movies","flicks","truefilm","moviesuggestions","cinema",
                      "letterboxd","bollywood","kollywood","tollywood",
                      "tamilmovies","telugumovies","malayalammovies",
                      "bollywoodmusicvideos","malayalammusic"}

        # get list of all movie names to detect when comments mention OTHER movies
        all_movie_names = set(df["movie_name"].str.lower().unique())
        pick_words = set(pick.lower().split())

        def is_relevant(row):
            text = str(row["comment_text"]).lower()
            sub = str(row.get("subreddit", "")).lower()

            # best case: comment mentions this movie by name
            mentions_this = pick.lower() in text
            if mentions_this:
                return True

            # has explicit rating → probably about the movie
            if pd.notna(row.get("extracted_rating")):
                return True

            # check if comment mentions a DIFFERENT movie but not this one
            # catches "best biopic" threads where ppl discuss other movies
            for other_movie in all_movie_names:
                if other_movie == pick.lower():
                    continue
                # skip short names that could match random words
                if len(other_movie) < 5:
                    continue
                if other_movie in text and not mentions_this:
                    return False

            # on movie subs, accept if it has movie discussion language
            if sub in movie_subs:
                return any(s in text for s in movie_signals)

            # general subs need movie signals
            return any(s in text for s in movie_signals)

        if len(m_comments) > 0:
            m_comments["_relevant"] = m_comments.apply(is_relevant, axis=1)
            relevant = m_comments[m_comments["_relevant"]]
            # only use filtered set if we still have enough comments
            if len(relevant) >= 3:
                m_comments = relevant.drop(columns=["_relevant"])
            else:
                m_comments = m_comments.drop(columns=["_relevant"])

        total_raw = len(comments[comments["movie_name"] == pick])
        st.caption(f"showing {len(m_comments)} relevant comments (of {total_raw} scraped) for '{pick}'")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Reddit", f"{m['reddit_score']:.2f}")
        c2.metric("IMDb", f"{m['imdb_rating']:.1f}")
        rt_val = m.get("rt_score_10", None)
        c3.metric("RT (/10)", f"{rt_val:.1f}" if pd.notna(rt_val) else "N/A")
        c4.metric("Comments", f"{m['total_comments']:.0f}")

        st.write(f"**Genre:** {m.get('genre','N/A')} | **Region:** {m.get('region','N/A')}")

        if len(m_comments) > 0:
            left, right = st.columns(2)

            with left:
                if "multi_label" in m_comments.columns:
                    counts = m_comments["multi_label"].value_counts()
                    fig = px.pie(values=counts.values, names=counts.index,
                                title="sentiment breakdown",
                                color=counts.index,
                                color_discrete_map={"positive":"#22c55e","neutral":"#94a3b8","negative":"#ef4444"})
                    st.plotly_chart(fig, use_container_width=True)

            with right:
                if "multi_score" in m_comments.columns:
                    fig = px.histogram(m_comments, x="multi_score", nbins=15,
                                      title="score distribution", color_discrete_sequence=["#FF4500"])
                    st.plotly_chart(fig, use_container_width=True)

            # top comments — drop dupes first
            st.subheader("top comments")
            top = m_comments.drop_duplicates(subset=["comment_text"]).nlargest(5, "upvotes")[
                ["comment_text","upvotes","multi_label","subreddit"]
            ].copy()
            top.columns = ["Comment","Upvotes","Sentiment","Sub"]
            top["Comment"] = top["Comment"].str[:200] + "..."
            st.dataframe(top, hide_index=True, use_container_width=True)
        else:
            st.info("no comments found for this movie")


# ===== TAB 3: sentiment =====
with tab3:
    left, right = st.columns(2)

    with left:
        models = pd.DataFrame({
            "Model": ["VADER","TextBlob","RoBERTa","Multi BERT"],
            "MAE": [2.65, 1.84, 2.45, 1.99],
            "Type": ["Lexicon","Lexicon","Transformer","Transformer"]
        })
        fig = px.bar(models, x="Model", y="MAE", color="Type",
                    title="MAE by model (lower = better)",
                    color_discrete_map={"Lexicon":"#94a3b8","Transformer":"#FF4500"})
        st.plotly_chart(fig, use_container_width=True)

    with right:
        models["Corr"] = [0.251, 0.335, 0.448, 0.467]
        fig = px.bar(models, x="Model", y="Corr", color="Type",
                    title="correlation w/ explicit ratings (higher = better)",
                    color_discrete_map={"Lexicon":"#94a3b8","Transformer":"#FF4500"})
        st.plotly_chart(fig, use_container_width=True)

    # languages
    st.subheader("languages detected")
    if "language" in comments.columns:
        langs = comments["language"].value_counts().head(10)
        fig = px.bar(x=langs.index, y=langs.values, title="top 10 comment languages",
                    color_discrete_sequence=["#4ECDC4"])
        fig.update_layout(xaxis_title="Language", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

        eng = (comments["language"]=="en").mean() * 100
        st.info(f"{eng:.1f}% english, {100-eng:.1f}% non-english (handled by multilingual bert)")

    # sentiment over time
    st.subheader("sentiment over time")
    score_col = "multi_score" if "multi_score" in comments.columns else "final_score_v2"
    monthly = comments.groupby(comments["comment_timestamp"].dt.to_period("M")).agg(
        avg=(score_col, "mean"), n=("movie_name", "count")
    ).reset_index()
    monthly["month"] = monthly["comment_timestamp"].astype(str)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=monthly["month"], y=monthly["avg"],
                  name="avg score", line=dict(color="#FF4500", width=2)), secondary_y=False)
    fig.add_trace(go.Bar(x=monthly["month"], y=monthly["n"],
                  name="volume", marker_color="rgba(69,183,209,0.3)"), secondary_y=True)
    fig.update_layout(title="monthly sentiment + volume", height=350)
    st.plotly_chart(fig, use_container_width=True)


# ===== TAB 4: predictions =====
with tab4:
    st.subheader("can reddit predict imdb ratings?")

    c1, c2, c3 = st.columns(3)
    c1.metric("Random Forest MAE", "0.683")
    c2.metric("Baseline (just guess avg)", "0.752")
    c3.metric("Improvement", "9.1%", delta="0.069")
    
    # feature importance
    st.subheader("which reddit features matter?")
    feats = pd.DataFrame({
        "feature": ["negative_ratio","avg_score","sentiment_shift","std_score",
                    "controversy","positive_ratio","avg_upvotes","max_upvotes",
                    "comment_count","neutral_ratio","median_score","log_comments"],
        "importance": [0.229,0.142,0.115,0.110,0.086,0.064,0.062,0.056,0.048,0.040,0.030,0.018]
    })
    fig = px.bar(feats.sort_values("importance"), x="importance", y="feature",
                orientation="h", title="feature importance (random forest)",
                color_discrete_sequence=["#45B7D1"])
    fig.update_layout(height=380)
    st.plotly_chart(fig, use_container_width=True)

    # early sentiment
    st.subheader("early prediction")
    early = pd.DataFrame({
        "window": ["7 days","14 days","30 days","all data"],
        "MAE": [0.787, 0.805, 0.784, 0.683],
        "corr": [0.203, 0.197, 0.227, 0.348]
    })
    st.dataframe(early, hide_index=True, use_container_width=True)
    st.caption("even first-week comments get similar accuracy — the signal shows up immediately")


# footer
st.markdown("---")
st.caption("data: tmdb + omdb + reddit | 26k comments | 571 movies | snowflake | "
           "[github](https://github.com/athulyabiju23/moviepulse)| [Tableau Dashboard](https://public.tableau.com/app/profile/athulya.biju/viz/Book1_17727323861070/Dashboard1)")