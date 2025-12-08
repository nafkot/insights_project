import os
import sys
import json
import sqlite3
from collections import Counter
from datetime import datetime
from flask import Flask, render_template, request, g, jsonify, url_for, redirect
from dotenv import load_dotenv
from openai import OpenAI
import time
import random

# Add parent directory to path to import config
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(BASE_DIR)

from utils.search_engine import answer_user_query
from utils.autocomplete import hybrid_autocomplete, llm_semantic_suggestions
from web.qa import ask_insights_llm

load_dotenv()

app = Flask(__name__, static_folder="static", template_folder="templates")
DB_PATH = os.getenv("YOUTUBE_DB", "youtube_insights.db")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

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

# --- INTELLIGENCE HELPERS ---

def get_channel_overview(conn, channel_id, channel_title, videos):
    """Generates a text summary of the channel based on video summaries."""
    cache_key = f"channel:{channel_id}:overview"

    # 1. Check Cache
    cached = conn.execute("SELECT payload FROM cached_dashboards WHERE key = ?", (cache_key,)).fetchone()
    if cached:
        return json.loads(cached['payload'])

    # 2. Prepare Data
    summaries = [v['overall_summary'] for v in videos if v['overall_summary']]
    if not summaries:
        return "No video data available to generate a summary."

    context_text = "\n- ".join(summaries[:20])

    prompt = f"""
    You are a YouTube Strategy Analyst.
    Analyze these video summaries from the creator "{channel_title}":

    {context_text}

    Write a 2-paragraph "Channel Strategy Overview" describing:
    1. The main content themes and niches.
    2. The creator's style (e.g., educational, vlog-style, review-heavy).

    Keep it professional and insightful. HTML format (use <p> tags).
    """

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        overview = resp.choices[0].message.content

        conn.execute("""
            INSERT INTO cached_dashboards (key, type, payload, updated_at)
            VALUES (?, 'channel_overview', ?, datetime('now'))
            ON CONFLICT(key) DO UPDATE SET payload=excluded.payload, updated_at=datetime('now')
        """, (cache_key, json.dumps(overview)))
        conn.commit()

        return overview
    except Exception as e:
        print(f"Error generating channel overview: {e}")
        return "<p>Unable to generate analysis at this time.</p>"

def get_brand_intelligence(conn, brand_id, brand_name, context_data, last_mention):
    return {"brief": f"<p>Analysis for {brand_name}...</p>", "video_summaries": {}, "word_cloud": []}

def get_product_intelligence(conn, product_id, product_name, context_data, last_mention):
    return {"brief": f"<p>Analysis for {product_name}...</p>", "video_summaries": {}, "word_cloud": []}

# --- ROUTES ---

@app.route("/")
def home(): return render_template("home.html")

@app.route("/brands")
def brands_hub():
    """Brands Landing Page (The missing route!)"""
    conn = get_db()

    # 1. Trending Brands
    trending_brands = conn.execute("""
        SELECT b.name, b.id, count(bm.id) as cnt
        FROM brands b JOIN brand_mentions bm ON b.id = bm.brand_id
        GROUP BY b.id ORDER BY cnt DESC LIMIT 5
    """).fetchall()

    # 2. Trending Products
    trending_products = conn.execute("""
        SELECT p.name, p.id, b.name as brand_name, count(pm.id) as cnt
        FROM products p
        JOIN product_mentions pm ON p.id = pm.product_id
        LEFT JOIN brands b ON p.brand_id = b.id
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
        SELECT p.name, p.id, b.name as brand_name, AVG(pm.sentiment_score) as score, count(pm.id) as c
        FROM products p
        JOIN product_mentions pm ON p.id = pm.product_id
        LEFT JOIN brands b ON p.brand_id = b.id
        GROUP BY p.id HAVING c > 1 ORDER BY score DESC LIMIT 5
    """).fetchall()

    channels = conn.execute("SELECT channel_id, title, subscriber_count, platform FROM channels ORDER BY subscriber_count DESC LIMIT 10").fetchall()

    return render_template(
        "brands_landing.html",
        trending_brands=trending_brands,
        trending_products=trending_products,
        popular_brands=popular_brands,
        popular_products=popular_products,
        channels=channels
    )

@app.route("/channel/<channel_id>")
def channel_profile(channel_id):
    conn = get_db()
    channel = conn.execute("SELECT * FROM channels WHERE channel_id = ?", (channel_id,)).fetchone()
    if not channel: return "Channel not found", 404

    videos = conn.execute("SELECT * FROM videos WHERE channel_id = ? ORDER BY upload_date DESC", (channel_id,)).fetchall()

    stats_row = conn.execute("""
        SELECT
            (SELECT COUNT(DISTINCT brand_id) FROM brand_mentions WHERE channel_id = ?) as brand_count,
            (SELECT COUNT(DISTINCT product_id) FROM product_mentions WHERE channel_id = ?) as product_count
    """, (channel_id, channel_id)).fetchone()

    sents = [100 if "positive" in (v["overall_sentiment"] or "").lower() else 0 for v in videos]
    avg_sentiment = sum(sents)/len(sents) if sents else 50

    stats = {
        "video_count": len(videos),
        "sentiment_avg": avg_sentiment,
        "brand_count": stats_row['brand_count'],
        "product_count": stats_row['product_count']
    }

    # AI Channel Overview
    channel_overview = get_channel_overview(conn, channel_id, channel['title'], videos)

    top_brands = conn.execute("""
        SELECT b.id, b.name, COUNT(*) as cnt
        FROM brand_mentions bm JOIN brands b ON bm.brand_id = b.id
        WHERE bm.channel_id = ? GROUP BY b.id ORDER BY cnt DESC LIMIT 30
    """, (channel_id,)).fetchall()

    top_products = conn.execute("""
        SELECT p.id, p.name, b.name as brand_name, COUNT(*) as cnt
        FROM product_mentions pm JOIN products p ON pm.product_id = p.id
        LEFT JOIN brands b ON p.brand_id = b.id
        WHERE pm.channel_id = ? GROUP BY p.id ORDER BY cnt DESC LIMIT 30
    """, (channel_id,)).fetchall()

    brand_cloud = [{"text": b['name'], "weight": b['cnt']} for b in top_brands]
    product_cloud = [{"text": p['name'], "weight": p['cnt']} for p in top_products]

    all_topics = []
    for v in videos:
        if v['topics']:
            all_topics.extend([t.strip() for t in v['topics'].split(',') if t.strip()])
    topic_cloud = [{"text": t, "weight": c} for t, c in Counter(all_topics).most_common(40)]

    return render_template(
        "channel_profile.html",
        channel=channel,
        videos=videos,
        stats=stats,
        channel_overview=channel_overview,
        top_brands=top_brands[:6],
        top_products=top_products[:6],
        brand_cloud=brand_cloud,
        product_cloud=product_cloud,
        word_cloud_data=topic_cloud
    )

@app.route("/brands/all")
def brands_directory():
    conn = get_db()
    filter_channel = request.args.get('channel')
    sql = """
        SELECT b.id, b.name, b.category, COUNT(DISTINCT bm.id) as mention_count, COUNT(DISTINCT p.id) as product_count
        FROM brands b LEFT JOIN brand_mentions bm ON b.id = bm.brand_id LEFT JOIN products p ON b.id = p.brand_id
    """
    params = []
    if filter_channel:
        sql = """
            SELECT b.id, b.name, b.category, COUNT(bm.id) as mention_count, (SELECT COUNT(*) FROM products p WHERE p.brand_id = b.id) as product_count
            FROM brands b JOIN brand_mentions bm ON b.id = bm.brand_id WHERE bm.channel_id = ?
        """
        params = [filter_channel]

    sql += " GROUP BY b.id ORDER BY mention_count DESC"
    return render_template("brands_list.html", brands=conn.execute(sql, params).fetchall(), filter_channel=filter_channel)

@app.route("/products/all")
def products_directory():
    conn = get_db()
    filter_channel = request.args.get('channel')
    sql = """
        SELECT p.id, p.name, b.name as brand_name, COUNT(pm.id) as mention_count
        FROM products p LEFT JOIN product_mentions pm ON p.id = pm.product_id LEFT JOIN brands b ON p.brand_id = b.id
    """
    params = []
    if filter_channel:
        sql = """
            SELECT p.id, p.name, b.name as brand_name, COUNT(pm.id) as mention_count
            FROM products p JOIN product_mentions pm ON p.id = pm.product_id LEFT JOIN brands b ON p.brand_id = b.id WHERE pm.channel_id = ?
        """
        params = [filter_channel]

    sql += " GROUP BY p.id ORDER BY mention_count DESC"
    return render_template("products_list.html", products=conn.execute(sql, params).fetchall(), filter_channel=filter_channel)

@app.route("/brand/<brand_id>")
def brand_profile(brand_id):
    conn = get_db()
    filter_channel = request.args.get('channel')

    brand = conn.execute("SELECT * FROM brands WHERE id = ?", (brand_id,)).fetchone()
    if not brand: return "Brand not found", 404

    metrics = conn.execute("SELECT COUNT(*) as total_mentions, COUNT(DISTINCT channel_id) as unique_channels, AVG(sentiment_score) as avg_sentiment, MAX(first_seen_date) as last_mentioned FROM brand_mentions WHERE brand_id = ?", (brand_id,)).fetchone()

    top_creator = conn.execute("SELECT channel_name, COUNT(*) as cnt FROM brand_mentions bm JOIN videos v ON bm.video_id = v.video_id WHERE bm.brand_id = ? GROUP BY v.channel_id ORDER BY cnt DESC LIMIT 1", (brand_id,)).fetchone()

    top_products = conn.execute("SELECT p.id, p.name, COUNT(pm.id) as cnt, COALESCE(AVG(pm.sentiment_score), 0) as score FROM products p LEFT JOIN product_mentions pm ON p.id = pm.product_id WHERE p.brand_id = ? GROUP BY p.id ORDER BY cnt DESC", (brand_id,)).fetchall()

    sql = "SELECT v.video_id, v.title, v.channel_name, v.upload_date, v.thumbnail_url, bm.mention_count, bm.sentiment_score, v.overall_summary FROM brand_mentions bm JOIN videos v ON bm.video_id = v.video_id WHERE bm.brand_id = ?"
    params = [brand_id]
    if filter_channel:
        sql += " AND bm.channel_id = ?"
        params.append(filter_channel)
    sql += " ORDER BY v.upload_date DESC"

    videos_rows = conn.execute(sql, params).fetchall()

    videos = []
    llm_input = []
    for row in videos_rows:
        vid = dict(row)
        matches = conn.execute("SELECT text FROM video_segments WHERE video_id = ? AND lower(text) LIKE ? LIMIT 3", (vid['video_id'], f"%{brand['name'].lower()}%")).fetchall()
        snippet = " ... ".join([m['text'] for m in matches]) if matches else "Brand mentioned in video."
        vid['raw_snippet'] = snippet
        vid['display_summary'] = vid.get('overall_summary') or snippet
        llm_input.append({"video_id": vid['video_id'], "date": vid['upload_date'], "text": snippet})
        videos.append(vid)

    intelligence = get_brand_intelligence(conn, brand_id, brand['name'], llm_input, metrics['last_mentioned'])

    timeline = conn.execute("SELECT date(first_seen_date) as day, COUNT(*) as cnt, AVG(sentiment_score) as score FROM brand_mentions WHERE brand_id = ? GROUP BY day ORDER BY day ASC", (brand_id,)).fetchall()

    return render_template("brand_profile.html", brand=brand, metrics=metrics, top_creator=top_creator, top_products=top_products, videos=videos, marketing_brief=intelligence.get('brief'), marketing_brief_data=intelligence, chart_labels=[r['day'] for r in timeline], chart_mentions=[r['cnt'] for r in timeline], chart_sentiment=[r['score'] for r in timeline], filter_channel_id=filter_channel)

@app.route("/product/<int:product_id>")
def product_profile(product_id):
    conn = get_db()
    product = conn.execute("SELECT p.id, p.name, b.id AS brand_id, b.name AS brand_name FROM products p LEFT JOIN brands b ON p.brand_id = b.id WHERE p.id = ?", (product_id,)).fetchone()
    if not product: return "Product not found", 404

    metrics = conn.execute("SELECT COUNT(*) as total_mentions, COUNT(DISTINCT channel_id) as unique_channels, AVG(sentiment_score) as avg_sentiment, MAX(first_seen_date) as last_mentioned FROM product_mentions WHERE product_id = ?", (product_id,)).fetchone()
    top_creator = conn.execute("SELECT channel_name, COUNT(*) as cnt FROM product_mentions pm JOIN videos v ON pm.video_id = v.video_id WHERE pm.product_id = ? GROUP BY v.channel_id ORDER BY cnt DESC LIMIT 1", (product_id,)).fetchone()

    videos_rows = conn.execute("SELECT v.video_id, v.title, v.channel_name, v.upload_date, v.thumbnail_url, pm.mention_count, pm.sentiment_score, v.overall_summary FROM product_mentions pm JOIN videos v ON pm.video_id = v.video_id WHERE pm.product_id = ? ORDER BY v.upload_date DESC", (product_id,)).fetchall()

    videos = []
    llm_input = []
    for row in videos_rows:
        vid = dict(row)
        matches = conn.execute("SELECT text FROM video_segments WHERE video_id = ? AND lower(text) LIKE ? LIMIT 3", (vid['video_id'], f"%{product['name'].lower()}%")).fetchall()
        snippet = " ... ".join([m['text'] for m in matches]) if matches else "Mentioned in video."
        vid['raw_snippet'] = snippet
        vid['display_summary'] = vid.get('overall_summary') or snippet
        llm_input.append({"video_id": vid['video_id'], "date": vid['upload_date'], "text": snippet})
        videos.append(vid)

    intelligence = get_product_intelligence(conn, product_id, product['name'], llm_input, metrics['last_mentioned'])
    timeline = conn.execute("SELECT date(first_seen_date) as day, COUNT(*) as cnt, AVG(sentiment_score) as score FROM product_mentions WHERE product_id = ? GROUP BY day ORDER BY day ASC", (product_id,)).fetchall()

    return render_template("product_profile.html", product=product, metrics=metrics, top_creator=top_creator, videos=videos, marketing_brief_data=intelligence, chart_labels=[r['day'] for r in timeline], chart_mentions=[r['cnt'] for r in timeline], chart_sentiment=[r['score'] for r in timeline])

@app.route("/video/<video_id>")
def video_profile(video_id):
    conn = get_db()
    video = conn.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,)).fetchone()
    if not video: return "Video not found", 404
    segments = conn.execute("SELECT * FROM video_segments WHERE video_id = ? ORDER BY start_time ASC", (video_id,)).fetchall()
    brands = conn.execute("SELECT DISTINCT b.id, b.name FROM brand_mentions bm JOIN brands b ON bm.brand_id = b.id WHERE bm.video_id = ?", (video_id,)).fetchall()
    products = conn.execute("SELECT DISTINCT p.id, p.name FROM product_mentions pm JOIN products p ON pm.product_id = p.id WHERE pm.video_id = ?", (video_id,)).fetchall()
    return render_template("video_profile.html", video=video, segments=segments, brands=brands, products=products)

@app.route("/autocomplete")
def autocomplete():
    query = request.args.get("q", "").strip().lower()
    conn = get_db()
    results = {"channels": [], "brands": [], "products": [], "semantic": []}
    if not query: return jsonify(results)

    def get_matches(t, cid, cname, lim=5): return conn.execute(f"SELECT {cid}, {cname} FROM {t} WHERE lower({cname}) LIKE ? LIMIT ?", (f"%{query}%", lim)).fetchall()
    for r in get_matches("channels", "channel_id", "title"): results["channels"].append({"id": r[0], "name": r[1], "platform": "YouTube"})
    for r in get_matches("brands", "id", "name"): results["brands"].append({"id": r[0], "name": r[1]})
    for r in get_matches("products", "id", "name"): results["products"].append({"id": r[0], "name": r[1]})

    if sum(len(v) for v in results.values()) < 3:
        semantic = llm_semantic_suggestions(query)
        results["semantic"] = [{"id": None, "name": s} for s in semantic]
    return jsonify(results)

@app.route("/api/qa", methods=["POST"])
def api_qa():
    data = request.json
    context_type = data.get("context_type")
    context_name = data.get("context_name")
    question = data.get("question")
    aggregates = data.get("aggregates", {})
    segments = data.get("segments", [])
    answer = ask_insights_llm(context_type, context_name, question, aggregates, segments)
    return jsonify({"answer": answer})

@app.route("/admin")
def admin_dashboard():
    conn = get_db()
    counts = {
        "channels": conn.execute("SELECT count(*) FROM channels").fetchone()[0],
        "videos": conn.execute("SELECT count(*) FROM videos").fetchone()[0],
        "transcripts": conn.execute("SELECT count(DISTINCT video_id) FROM video_segments").fetchone()[0],
        "failed": conn.execute("SELECT count(*) FROM ingestion_logs WHERE status='FAILED'").fetchone()[0]
    }
    logs = conn.execute("SELECT * FROM ingestion_logs ORDER BY timestamp DESC LIMIT 50").fetchall()
    channels = conn.execute("SELECT title, video_count, platform FROM channels ORDER BY video_count DESC").fetchall()
    return render_template("admin_dashboard.html", counts=counts, logs=logs, channels=channels)

@app.route("/search")
def search():
    query = request.args.get("q", "").strip()
    if not query: return redirect(url_for("home"))
    conn = get_db()
    q_like = f"%{query}%"
    sql = "SELECT video_id, title, channel_name, thumbnail_url, upload_date, overall_summary FROM videos WHERE title LIKE ? ORDER BY upload_date DESC LIMIT 20"
    rows = conn.execute(sql, (q_like,)).fetchall()
    videos = [dict(row) for row in rows]
    return render_template("search.html", query=query, videos=videos,
                           channels=conn.execute("SELECT * FROM channels WHERE title LIKE ? LIMIT 5", (q_like,)).fetchall(),
                           brands=conn.execute("SELECT * FROM brands WHERE name LIKE ? LIMIT 5", (q_like,)).fetchall(),
                           products=conn.execute("SELECT * FROM products WHERE name LIKE ? LIMIT 5", (q_like,)).fetchall(),
                           ai_answer=answer_user_query(query))

@app.context_processor
def inject_categories():
    return dict(navbar_categories=["Autos & Vehicles", "Beauty", "Comedy", "Education", "Entertainment", "Gaming", "Howto & Style", "Music", "News & Politics", "People & Blogs", "Pets & Animals", "Science & Technology", "Sports", "Travel & Events"])

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
