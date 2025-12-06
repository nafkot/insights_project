import os
import sys
import json
import sqlite3
from collections import Counter
from flask import Flask, render_template, request, g, jsonify, url_for, redirect
from dotenv import load_dotenv
from openai import OpenAI
import markdown

# ------------------------------------------------------------------------------
# CRITICAL FIX: Add the parent directory to sys.path to allow importing 'utils'
# ------------------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(BASE_DIR)

# --- Local Imports ---
from utils.search_engine import answer_user_query
from utils.autocomplete import hybrid_autocomplete, llm_semantic_suggestions

load_dotenv()

app = Flask(__name__, static_folder="static", template_folder="templates")

# Ensure DB path is correct relative to where the script is run
DB_PATH = os.getenv("YOUTUBE_DB", "youtube_insights.db")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)


# -------------------------------
# Database Connection
# -------------------------------
def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()

# -------------------------------
# Template Filters
# -------------------------------
@app.template_filter("from_json")
def from_json_filter(value):
    try:
        return json.loads(value)
    except:
        return []

# -------------------------------
# Context Processor (Navbar Categories)
# -------------------------------
@app.context_processor
def inject_categories():
    """
    Injects the full list of YouTube categories into the navbar.
    Combines DB data with a hardcoded fallback list.
    """
    try:
        # 1. Start with the official YouTube category list
        full_categories = [
            "Autos & Vehicles", "Comedy", "Education", "Entertainment", "Film & Animation",
            "Gaming", "Howto & Style", "Music", "News & Politics", "Nonprofits & Activism",
            "People & Blogs", "Pets & Animals", "Science & Technology", "Sports", "Travel & Events"
        ]

        # 2. Optionally, check DB for any custom/new ones
        conn = get_db()
        rows = conn.execute("SELECT DISTINCT category FROM channels WHERE category IS NOT NULL AND category != ''").fetchall()
        db_cats = [r['category'] for r in rows]

        # Merge and sort unique categories
        final_cats = sorted(list(set(full_categories + db_cats)))

        return dict(navbar_categories=final_cats)

    except Exception as e:
        print(f"Error loading categories: {e}")
        # Fallback if DB fails
        return dict(navbar_categories=[
            "Autos & Vehicles", "Comedy", "Education", "Entertainment", "Film & Animation",
            "Gaming", "Howto & Style", "Music", "News & Politics", "Nonprofits & Activism",
            "People & Blogs", "Pets & Animals", "Science & Technology", "Sports", "Travel & Events"
        ])


# -------------------------------
# Routes
# -------------------------------

@app.route("/", methods=["GET"])
def home():
    """Simple Google-style landing page (Search Only)."""
    return render_template("home.html")


@app.route("/brands", methods=["GET"])
def brands_hub():
    """The main Dashboard for Brands."""
    conn = get_db()

    # 1. Trending Brands
    trending_brands = conn.execute("""
        SELECT b.name, b.id, count(bm.id) as cnt
        FROM brands b JOIN brand_mentions bm ON b.id = bm.brand_id
        GROUP BY b.id ORDER BY cnt DESC LIMIT 5
    """).fetchall()

    # 2. Trending Products
    trending_products = conn.execute("""
        SELECT p.name, p.id, count(pm.id) as cnt
        FROM products p JOIN product_mentions pm ON p.id = pm.product_id
        GROUP BY p.id ORDER BY cnt DESC LIMIT 5
    """).fetchall()

    # 3. Popular Brands
    popular_brands = conn.execute("""
        SELECT b.name, b.id, AVG(bm.sentiment_score) as score, count(bm.id) as c
        FROM brands b JOIN brand_mentions bm ON b.id = bm.brand_id
        GROUP BY b.id HAVING c > 1 ORDER BY score DESC LIMIT 5
    """).fetchall()

    # 4. Popular Products
    popular_products = conn.execute("""
        SELECT p.name, p.id, AVG(pm.sentiment_score) as score, count(pm.id) as c
        FROM products p JOIN product_mentions pm ON p.id = pm.product_id
        GROUP BY p.id HAVING c > 1 ORDER BY score DESC LIMIT 5
    """).fetchall()

    # 5. Top Channels
    channels = conn.execute("""
        SELECT channel_id, title, subscriber_count, platform
        FROM channels ORDER BY subscriber_count DESC LIMIT 10
    """).fetchall()

    return render_template(
        "brands_landing.html",
        trending_brands=trending_brands,
        trending_products=trending_products,
        popular_brands=popular_brands,
        popular_products=popular_products,
        channels=channels
    )


@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("q", "").strip()
    if not query:
        return redirect(url_for("home"))

    conn = get_db()
    videos = []

    rows = conn.execute(
        "SELECT video_id, title, channel_name, thumbnail_url FROM videos WHERE title LIKE ? ORDER BY upload_date DESC LIMIT 30",
        (f"%{query}%",)
    ).fetchall()

    for row in rows:
        videos.append({
            "video_id": row["video_id"],
            "title": row["title"],
            "channel_name": row["channel_name"],
            "thumbnail_url": row["thumbnail_url"]
        })

    ai_answer = answer_user_query(query)

    return render_template("search.html", query=query, videos=videos, ai_answer=ai_answer)


@app.route("/channel/<channel_id>")
def channel_profile(channel_id):
    conn = get_db()
    channel = conn.execute("SELECT * FROM channels WHERE channel_id = ?", (channel_id,)).fetchone()
    if not channel: return "Channel not found", 404

    videos = conn.execute("SELECT * FROM videos WHERE channel_id = ? ORDER BY upload_date DESC", (channel_id,)).fetchall()

    # Basic aggregations
    brand_counts = Counter()
    product_counts = Counter()
    sentiments = []
    topics_list = []

    for v in videos:
        if v["topics"]: topics_list.extend(v["topics"].split(","))
        try:
            if v["brands"]: brand_counts.update(json.loads(v["brands"]))
            if v["products"]: product_counts.update([p.get("product") for p in json.loads(v["products"])])
        except: pass

        s = (v["overall_sentiment"] or "").lower()
        sentiments.append(100 if "positive" in s else (0 if "negative" in s else 50))

    stats = {
        "top_topics": [t[0] for t in Counter(topics_list).most_common(10)],
        "top_brands": [b[0] for b in brand_counts.most_common(10)],
        "top_products": [p[0] for p in product_counts.most_common(10)],
        "brand_count": len(brand_counts),
        "product_count": len(product_counts),
        "video_count": len(videos),
        "sentiment_avg": sum(sentiments)/len(sentiments) if sentiments else 50
    }

    return render_template("channel_profile.html", channel=channel, videos=videos, stats=stats)


@app.route("/brand/<brand_id>")
def brand_profile(brand_id):
    conn = get_db()
    brand = conn.execute("SELECT * FROM brands WHERE id = ?", (brand_id,)).fetchone()
    if not brand: return "Brand not found", 404

    mentions = conn.execute("""
        SELECT v.title, v.channel_name, v.video_id, bm.first_seen_date, bm.sentiment_score
        FROM brand_mentions bm
        JOIN videos v ON bm.video_id = v.video_id
        WHERE bm.brand_id = ?
        ORDER BY bm.first_seen_date DESC
    """, (brand_id,)).fetchall()

    products = conn.execute("SELECT * FROM products WHERE brand_id = ?", (brand_id,)).fetchall()

    return render_template("brand_profile.html", brand=brand, videos=mentions, products=products)


# ... (keep existing imports) ...

# --- Helper: Generate Marketing Brief ---
def generate_marketing_brief(product_name, context_segments):
    """
    Uses LLM to generate a marketing summary based on actual mentions.
    context_segments: list of dicts {date, text, sentiment}
    """
    if not context_segments:
        return None

    # Limit to last 40 mentions to fit in context window and stay fast
    recent_segments = context_segments[:40]

    # Prepare data for LLM
    context_text = ""
    for s in recent_segments:
        context_text += f"- [{s['date']}] ({s['sentiment']}): {s['text']}\n"

    prompt = f"""
    You are a Senior Marketing Analyst.
    Analyze these social media discussions about the product "{product_name}".

    DATA:
    {context_text}

    TASK:
    Write a concise "Marketing Intelligence Brief" (HTML format, no markdown blocks).

    Structure:
    <h3>1. Executive Summary</h3>
    <p>...general consensus...</p>

    <h3>2. Key Themes & Sentiment</h3>
    <ul>
      <li>...point 1...</li>
      <li>...point 2...</li>
    </ul>

    <h3>3. Timing & Trends</h3>
    <p>...are these recent? is there a shift in sentiment?...</p>

    Keep it professional, insightful, and under 250 words.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini", # or gpt-4-turbo / gpt-3.5-turbo
            messages=[
                {"role": "system", "content": "You are a helpful marketing analyst."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error generating brief: {e}")
        return None


# --- Route: Product Profile ---
@app.route("/product/<int:product_id>")
def product_profile(product_id):
    conn = get_db()

    # 1. Product Details
    product = conn.execute("""
        SELECT p.id, p.name, b.id AS brand_id, b.name AS brand_name
        FROM products p
        LEFT JOIN brands b ON p.brand_id = b.id
        WHERE p.id = ?
    """, (product_id,)).fetchone()

    if not product: return "Product not found", 404

    # 2. Metrics
    metrics = conn.execute("""
        SELECT
            COUNT(*) as total_mentions,
            COUNT(DISTINCT channel_id) as unique_channels,
            AVG(sentiment_score) as avg_sentiment,
            MAX(first_seen_date) as last_mentioned
        FROM product_mentions
        WHERE product_id = ?
    """, (product_id,)).fetchone()

    # 3. Top Creator
    top_creator = conn.execute("""
        SELECT channel_name, COUNT(*) as cnt
        FROM product_mentions pm
        JOIN videos v ON pm.video_id = v.video_id
        WHERE pm.product_id = ?
        GROUP BY v.channel_id
        ORDER BY cnt DESC LIMIT 1
    """, (product_id,)).fetchone()

    # 4. Fetch Videos AND Specific Context Segments
    # We first find the videos
    videos_rows = conn.execute("""
        SELECT
            v.video_id, v.title, v.channel_name, v.upload_date, v.thumbnail_url,
            pm.mention_count, pm.sentiment_score
        FROM product_mentions pm
        JOIN videos v ON pm.video_id = v.video_id
        WHERE pm.product_id = ?
        ORDER BY v.upload_date DESC
    """, (product_id,)).fetchall()

    videos = []
    all_segments_for_llm = []

    # For each video, find the specific text segment where product is mentioned
    for row in videos_rows:
        vid = dict(row)

        # Heuristic: Find segments containing the product name
        # In a production app, you might use FTS or store the specific segment_id in product_mentions
        matches = conn.execute("""
            SELECT text, start_time FROM video_segments
            WHERE video_id = ? AND lower(text) LIKE ? LIMIT 3
        """, (vid['video_id'], f"%{product['name'].lower()}%")).fetchall()

        # Fallback: if no direct string match (e.g. slight variation), grab the first 3 segments
        # or handle gracefully.

        context_snippets = [m['text'] for m in matches]
        vid['context'] = " ... ".join(context_snippets) if context_snippets else "Product mentioned in this video."

        # Add to list for LLM
        if context_snippets:
            all_segments_for_llm.append({
                "date": vid['upload_date'],
                "text": vid['context'],
                "sentiment": "Positive" if vid['sentiment_score'] > 60 else "Negative"
            })

        videos.append(vid)

    # 5. Generate Marketing Brief (Live LLM Call)
    # Only if we have data
    marketing_brief = None
    if all_segments_for_llm:
        marketing_brief = generate_marketing_brief(product['name'], all_segments_for_llm)

    return render_template(
        "product_profile.html",
        product=product,
        metrics=metrics,
        top_creator=top_creator,
        videos=videos,
        marketing_brief=marketing_brief
    )


# --- NEW ROUTE: VIDEO PROFILE ---
@app.route("/video/<video_id>")
def video_profile(video_id):
    conn = get_db()
    video = conn.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,)).fetchone()
    if not video:
        return "Video not found", 404

    # Transcript segments
    segments = conn.execute("SELECT * FROM video_segments WHERE video_id = ? ORDER BY start_time ASC", (video_id,)).fetchall()

    # Extracted Brands
    brands = conn.execute("""
        SELECT DISTINCT b.id, b.name
        FROM brand_mentions bm
        JOIN brands b ON bm.brand_id = b.id
        WHERE bm.video_id = ?
    """, (video_id,)).fetchall()

    # Extracted Products
    products = conn.execute("""
        SELECT DISTINCT p.id, p.name
        FROM product_mentions pm
        JOIN products p ON pm.product_id = p.id
        WHERE pm.video_id = ?
    """, (video_id,)).fetchall()

    return render_template("video_profile.html", video=video, segments=segments, brands=brands, products=products)


@app.route("/autocomplete")
def autocomplete():
    query = request.args.get("q", "").strip().lower()
    conn = get_db()

    results = {"channels": [], "brands": [], "products": [], "semantic": []}
    if not query: return jsonify(results)

    def get_matches(table, col_id, col_name, limit=5):
        return conn.execute(f"SELECT {col_id}, {col_name} FROM {table} WHERE lower({col_name}) LIKE ? LIMIT ?", (f"%{query}%", limit)).fetchall()

    for r in get_matches("channels", "channel_id", "title"):
        results["channels"].append({"id": r[0], "name": r[1], "platform": "YouTube"}) # Default platform for now

    for r in get_matches("brands", "id", "name"):
        results["brands"].append({"id": r[0], "name": r[1]})

    for r in get_matches("products", "id", "name"):
        results["products"].append({"id": r[0], "name": r[1]})

    if sum(len(v) for v in results.values()) < 3:
        semantic = llm_semantic_suggestions(query)
        results["semantic"] = [{"id": None, "name": s} for s in semantic]

    return jsonify(results)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
