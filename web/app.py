# app.py — Insights Project Web Application
#
# This file is a modernised version of your old app.py while keeping:
#   - Same UX flow
#   - Same DB logic style
#   - New schema support (brands, products, sponsors)
#   - LLM-enhanced search
#   - Channel, Brand, Product profile pages
#
# Fully compatible with your ingestion pipeline.

import os
import sys
import json
import sqlite3
from collections import Counter
from flask import Flask, render_template, request, g, jsonify
from dotenv import load_dotenv
from openai import OpenAI
import markdown
from flask import Flask, render_template, request, jsonify, url_for, redirect

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(BASE_DIR)

from utils.search_engine import answer_user_query
from utils.autocomplete import hybrid_autocomplete


load_dotenv()

app = Flask(__name__, static_folder="static", template_folder="templates")


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
# Autocomplete Search (Channel, Brand, Product)
# -------------------------------

# -------------------------------
# LLM Keyword Extractor
# -------------------------------

def extract_keywords(user_query):
    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Extract 1-3 search keywords."},
                {"role": "user", "content": user_query},
            ],
            temperature=0,
        )
        return resp.choices[0].message.content.strip()
    except:
        return user_query


# -------------------------------
# LLM Search Answer Generator
# -------------------------------

def generate_answer(user_query, segments, is_comparison=False):
    if not segments:
        return None

    context = ""
    channels = set()

    for seg in segments[:40]:
        channels.add(seg["channel_name"])
        context += (
            f"SOURCE: {seg['channel_name']} ({seg['start_time']}s)\n"
            f"QUOTE: {seg['text']}\n\n"
        )

    system_prompt = "You are an insights analyst."

    if is_comparison:
        system_prompt += (
            "\nCompare these channels strictly based on evidence. "
            "Cite timestamps. Structure as:\n"
            "Comparison:\nContrast:\n"
        )

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": f"Question: {user_query}\n\nEvidence:\n{context}",
                },
            ],
            temperature=0,
        )
        return markdown.markdown(resp.choices[0].message.content)
    except:
        return None


# -------------------------------
# Homepage Search
# -------------------------------

@app.route("/", methods=["GET"])
@app.route("/home", methods=["GET"])
def home():

# Parse channel_ids from query string
    channel_ids_str = request.args.get("channels", "")
    if channel_ids_str:
        current_channel_ids = [c.strip() for c in channel_ids_str.split(",") if c.strip()]
    else:
        current_channel_ids = None

    query = request.args.get("q", "").strip()
    channel_ids_str = request.args.get("channels", "")
    channel_ids = [c for c in channel_ids_str.split(",") if c]

    videos = []
    ai_answer = None

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Show recent videos if no query
    if not query:
        c.execute("SELECT video_id, title, channel_name FROM videos ORDER BY upload_date DESC LIMIT 30")
        for row in c.fetchall():
            videos.append({
                "video_id": row[0],
                "title": row[1],
                "channel_name": row[2]
            })

    # User submitted a query → run AI analysis
    else:
        # Basic DB search
        c.execute(
            "SELECT video_id, title, channel_name FROM videos WHERE title LIKE ? LIMIT 30",
            (f"%{query}%",)
        )
        for row in c.fetchall():
            videos.append({
                "video_id": row[0],
                "title": row[1],
                "channel_name": row[2]
            })

        # AI answer using your LLM pipeline
        ai_answer = answer_user_query(query, channel_ids=current_channel_ids)

    conn.close()

    return render_template(
        "home.html",
        videos=videos,
        query=query,
        ai_answer=ai_answer,
        active_channels=channel_ids,
        current_channel_ids=channel_ids_str
    )

@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("q", "").strip()
    if not query:
        return redirect(url_for("home"))

    return redirect(url_for("home", q=query))


# -------------------------------
# CHANNEL PROFILE PAGE
# -------------------------------

@app.route("/channel/<channel_id>")
def channel_profile(channel_id):
    conn = get_db()

    channel = conn.execute(
        "SELECT * FROM channels WHERE channel_id = ?",
        (channel_id,),
    ).fetchone()

    if not channel:
        return "Channel not found", 404

    videos = conn.execute(
        "SELECT * FROM videos WHERE channel_id = ? ORDER BY upload_date DESC",
        (channel_id,),
    ).fetchall()

    topics = []
    brand_list = []
    product_list = []
    sponsor_list = []
    sentiments = []

    for v in videos:
        if v["topics"]:
            topics.extend(v["topics"].split(","))

        # Brand JSON
        if v["brands"]:
            brand_list.extend(json.loads(v["brands"]))

        # Product JSON
        if v["products"]:
            product_list.extend(json.loads(v["products"]))

        # Sponsor JSON
        if v["sponsors"]:
            sponsor_list.extend(json.loads(v["sponsors"]))

        # Sentiment → numeric
        if "Positive" in v["overall_sentiment"]:
            sentiments.append(100)
        elif "Negative" in v["overall_sentiment"]:
            sentiments.append(0)
        else:
            sentiments.append(50)

    stats = {
        "top_topics": [x[0] for x in Counter(topics).most_common(10)],
        "top_brands": [x[0] for x in Counter(brand_list).most_common(20)],
        "top_products": [x[0] for x in Counter([p["product"] for p in product_list]).most_common(20)],
        "top_sponsors": [x[0] for x in Counter(sponsor_list).most_common(10)],
        "sentiment_avg": sum(sentiments) / len(sentiments) if sentiments else 50,
    }

    return render_template(
        "channel_profile.html",
        channel=channel,
        videos=videos,
        stats=stats,
    )


# -------------------------------
# BRAND PROFILE PAGE
# -------------------------------

@app.route("/brand/<brand_id>")
def brand_profile(brand_id):
    conn = get_db()

    brand = conn.execute("SELECT * FROM brands WHERE id = ?", (brand_id,)).fetchone()
    if not brand:
        return "Brand not found", 404

    mentions = conn.execute(
        """
        SELECT v.title, v.channel_name, bm.first_seen_date, bm.sentiment_score
        FROM brand_mentions bm
        JOIN videos v ON bm.video_id = v.video_id
        WHERE bm.brand_id = ?
        ORDER BY bm.first_seen_date DESC
        """,
        (brand_id,),
    ).fetchall()

    return render_template(
        "brand_profile.html",
        brand=brand,
        mentions=mentions,
    )


# -------------------------------
# PRODUCT PROFILE PAGE
# -------------------------------
@app.route("/product/<int:product_id>")
def product_profile(product_id):
    conn = get_db()

    # Fetch product record
    product = conn.execute("""
        SELECT
            p.id, p.name,
            b.id AS brand_id, b.name AS brand_name
        FROM products p
        LEFT JOIN brands b ON p.brand_id = b.id
        WHERE p.id = ?
    """, (product_id,)).fetchone()

    if not product:
        return "Product not found", 404

    # Aggregate metrics
    metrics = conn.execute(
        """
        SELECT
            COUNT(*) as total_mentions,
            COUNT(DISTINCT channel_id) as unique_channels,
            AVG(sentiment_score) as avg_sentiment
        FROM product_mentions
        WHERE product_id = ?
        """,
        (product_id,)
    ).fetchone()

    # Videos
    videos = conn.execute(
        """
        SELECT v.video_id, v.title, v.channel_name
        FROM product_mentions pm
        JOIN videos v ON v.video_id = pm.video_id
        WHERE pm.product_id = ?
        ORDER BY pm.mention_count DESC
        LIMIT 20
        """,
        (product_id,)
    ).fetchall()

    conn.close()

    return render_template(
        "product_profile.html",
        product=product,
        metrics=metrics,
        videos=videos,
    )

@app.route("/autocomplete")
def autocomplete():
    query = request.args.get("q", "").strip().lower()
    conn = get_db()

    results = {
        "channels": [],
        "brands": [],
        "products": [],
        "sponsors": [],
        "semantic": []  # new group for LLM suggestions
    }

    if not query:
        return jsonify(results)

    # Channels
    rows = conn.execute(
        "SELECT channel_id, title FROM channels WHERE lower(title) LIKE ? LIMIT 10",
        (f"%{query}%",)
    ).fetchall()
    results["channels"] = [{"id": r["channel_id"], "name": r["title"]} for r in rows]

    # Brands
    rows = conn.execute(
        "SELECT id, name FROM brands WHERE lower(name) LIKE ? LIMIT 10",
        (f"%{query}%",)
    ).fetchall()
    results["brands"] = [{"id": r["id"], "name": r["name"]} for r in rows]

    # Products
    rows = conn.execute(
        "SELECT id, name FROM products WHERE lower(name) LIKE ? LIMIT 10",
        (f"%{query}%",)
    ).fetchall()
    results["products"] = [{"id": r["id"], "name": r["name"]} for r in rows]

    # Sponsors
    rows = conn.execute(
        "SELECT id, name FROM sponsors WHERE lower(name) LIKE ? LIMIT 10",
        (f"%{query}%",)
    ).fetchall()
    results["sponsors"] = [{"id": r["id"], "name": r["name"]} for r in rows]

    conn.close()

    # Count total DB hits
    total_hits = (
        len(results["channels"]) +
        len(results["brands"]) +
        len(results["products"]) +
        len(results["sponsors"])
    )

    # HYBRID MODE B:
    # If DB results are weak (<3), call LLM for semantic predictions.
    if total_hits < 3:
        semantic_suggestions = llm_semantic_suggestions(query)
        results["semantic"] = [{"id": None, "name": s} for s in semantic_suggestions]

    return jsonify(results)



# -------------------------------
# Run Server
# -------------------------------

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

