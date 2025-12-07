import os
import sys
import json
import sqlite3
from collections import Counter
from datetime import datetime
from flask import Flask, render_template, request, g, jsonify, url_for, redirect
from dotenv import load_dotenv
from openai import OpenAI
import markdown
import time
import random

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

@app.template_filter("from_json")
def from_json_filter(value):
    try: return json.loads(value)
    except: return []

# -------------------------------------------------------------------------
# INTELLIGENCE HELPERS (Product & Brand)
# -------------------------------------------------------------------------

def get_intel_common(conn, entity_type, entity_id, entity_name, context_data, last_mention_date):
    """
    Generic Intelligence Generator with Debugging.
    """
    if not context_data:
        print(f"[Debug] No context data for {entity_name}")
        return {"brief": None, "video_summaries": {}}

    cache_key = f"{entity_type}:{entity_id}:intel_v3"

    # 1. Check Cache
    cached = conn.execute("SELECT payload, updated_at FROM cached_dashboards WHERE key = ?", (cache_key,)).fetchone()
    if cached:
        if last_mention_date and cached['updated_at'] >= last_mention_date:
            try:
                print(f"[Debug] Returning cached intel for {entity_name}")
                return json.loads(cached['payload'])
            except: pass

    # 2. Prepare Data
    recent_batch = context_data[:15]
    prompt_items = "".join([f"DATE: {i['date']}\nVIDEO_ID: {i['video_id']}\nTXT: {i['text']}\n---\n" for i in recent_batch])

    role_desc = "Senior Brand Strategist" if entity_type == "brand" else "Product Marketing Manager"

    prompt = f"""
    You are a {role_desc}. Analyze social conversations around the {entity_type} "{entity_name}".

    INPUT DATA (Snippets):
    {prompt_items}

    TASK:
    1. Write a "Strategic Brief" (HTML).
    2. Generate a "Word Cloud" of 15-20 distinctive attributes, adjectives, or themes.

    CRITICAL RULES FOR WORD CLOUD:
    - **FORBIDDEN WORDS**: Do NOT include the brand name ("{entity_name}"), product names, or generic terms like "product", "brand", "video", "channel", "thing", "love", "like", "obsesed", "hate", "prefer".
    - **FOCUS ON ATTRIBUTES**: Look for specific descriptors about:
      - Texture (e.g., "creamy", "chalky", "sticky")
      - Performance (e.g., "long-lasting", "pigmented", "patchy")
      - Value (e.g., "overpriced", "affordable", "worth it")
      - Packaging (e.g., "bulky", "luxurious", "cheap")
    - **SENTIMENT**: accurately classify each word as positive/negative/neutral.

    JSON SCHEMA:
    {{
      "brief": "<div>...</div>",
      "video_summaries": {{ "vid_id": "..." }},
      "word_cloud": [
         {{ "text": "creamy", "sentiment": "positive", "weight": 5 }},
         {{ "text": "expensive", "sentiment": "negative", "weight": 4 }}
      ]
    }}
    """


    print(f"[LLM] Generating {entity_type} intelligence for {entity_name}...")

    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": "Return valid JSON only."}, {"role": "user", "content": prompt}],
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            raw_content = resp.choices[0].message.content

            # --- DEBUG PRINT ---
            print(f"[Debug LLM Response]: {raw_content[:200]}...")
            # -------------------

            result = json.loads(raw_content)

            # Fallback: Check if keys exist, if not, try to patch
            if 'brief' not in result:
                print("[Debug] 'brief' key missing! Keys found:", result.keys())
                # Try to find a similar key
                for k in result.keys():
                    if 'brief' in k or 'report' in k:
                        result['brief'] = result[k]
                        break

            # Save to Cache
            conn.execute("""
                INSERT INTO cached_dashboards (key, type, payload, updated_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT(key) DO UPDATE SET payload=excluded.payload, updated_at=datetime('now')
            """, (cache_key, 'intel', json.dumps(result)))
            conn.commit()
            return result

        except Exception as e:
            print(f"[LLM Error] Attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return {"brief": None, "video_summaries": {}}

    return {"brief": None}


# Wrappers
def get_product_intelligence(conn, pid, name, data, date): return get_intel_common(conn, "product", pid, name, data, date)
def get_brand_intelligence(conn, bid, name, data, date): return get_intel_common(conn, "brand", bid, name, data, date)


@app.context_processor
def inject_categories():
    return dict(navbar_categories=["Autos & Vehicles", "Beauty", "Comedy", "Education", "Entertainment", "Gaming", "Howto & Style", "Music", "News & Politics", "People & Blogs", "Pets & Animals", "Science & Technology", "Sports", "Travel & Events"])

# -------------------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------------------

@app.route("/")
def home(): return render_template("home.html")

@app.route("/brands")
def brands_hub():
    conn = get_db()

    # ... inside brands_hub ...

    # 1. Trending Brands (Reverted to standard count, no image subquery)
    trending_brands = conn.execute("""
        SELECT b.name, b.id, count(bm.id) as cnt
        FROM brands b JOIN brand_mentions bm ON b.id = bm.brand_id
        GROUP BY b.id ORDER BY cnt DESC LIMIT 5
    """).fetchall()

    # 2. Trending Products (Removed image_url)
    trending_products = conn.execute("""
        SELECT
            p.name, p.id,
            b.name as brand_name,
            count(pm.id) as cnt
        FROM products p
        JOIN product_mentions pm ON p.id = pm.product_id
        LEFT JOIN brands b ON p.brand_id = b.id
        GROUP BY p.id ORDER BY cnt DESC LIMIT 5
    """).fetchall()

    # ... (popular_brands remains the same) ...
    popular_brands = conn.execute("SELECT b.name, b.id, AVG(bm.sentiment_score) as score, count(bm.id) as c FROM brands b JOIN brand_mentions bm ON b.id = bm.brand_id GROUP BY b.id HAVING c > 1 ORDER BY score DESC LIMIT 5").fetchall()

    # 4. Popular Products (Removed image_url)
    popular_products = conn.execute("""
        SELECT
            p.name, p.id,
            b.name as brand_name,
            AVG(pm.sentiment_score) as score,
            count(pm.id) as c
        FROM products p
        JOIN product_mentions pm ON p.id = pm.product_id
        LEFT JOIN brands b ON p.brand_id = b.id
        GROUP BY p.id HAVING c > 1
        ORDER BY score DESC LIMIT 5
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

@app.route("/channels/all")
def channels_directory():
    conn = get_db()
    # Fetch channels with video counts
    channels = conn.execute("""
        SELECT * FROM channels ORDER BY subscriber_count DESC
    """).fetchall()
    return render_template("channels_list.html", channels=channels)

@app.route("/search")
def search():
    query = request.args.get("q", "").strip()
    if not query: return redirect(url_for("home"))
    conn = get_db()

    q_like = f"%{query}%"
    videos = [dict(row) for row in conn.execute("SELECT video_id, title, channel_name, thumbnail_url, upload_date, overall_summary FROM videos WHERE title LIKE ? ORDER BY upload_date DESC LIMIT 20", (q_like,)).fetchall()]

    return render_template("search.html", query=query, videos=videos,
                           channels=conn.execute("SELECT * FROM channels WHERE title LIKE ? LIMIT 5", (q_like,)).fetchall(),
                           brands=conn.execute("SELECT * FROM brands WHERE name LIKE ? LIMIT 5", (q_like,)).fetchall(),
                           products=conn.execute("SELECT * FROM products WHERE name LIKE ? LIMIT 5", (q_like,)).fetchall(),
                           ai_answer=answer_user_query(query))

@app.route("/channel/<channel_id>")
def channel_profile(channel_id):
    conn = get_db()
    channel = conn.execute("SELECT * FROM channels WHERE channel_id = ?", (channel_id,)).fetchone()
    if not channel: return "Channel not found", 404
    videos = conn.execute("SELECT * FROM videos WHERE channel_id = ? ORDER BY upload_date DESC", (channel_id,)).fetchall()

    # Simple stats
    sents = []
    for v in videos:
        s = (v["overall_sentiment"] or "").lower()
        sents.append(100 if "positive" in s else (0 if "negative" in s else 50))

    stats = {"video_count": len(videos), "sentiment_avg": sum(sents)/len(sents) if sents else 50}
    return render_template("channel_profile.html", channel=channel, videos=videos, stats=stats)

@app.route("/brand/<brand_id>")
def brand_profile(brand_id):
    conn = get_db()
    brand = conn.execute("SELECT * FROM brands WHERE id = ?", (brand_id,)).fetchone()
    if not brand: return "Brand not found", 404

    # 1. Metrics
    metrics = conn.execute("""
        SELECT COUNT(*) as total_mentions, COUNT(DISTINCT channel_id) as unique_channels,
               AVG(sentiment_score) as avg_sentiment, MAX(first_seen_date) as last_mentioned
        FROM brand_mentions WHERE brand_id = ?
    """, (brand_id,)).fetchone()

    # 2. Top Creator
    top_creator = conn.execute("""
        SELECT channel_name, COUNT(*) as cnt FROM brand_mentions bm
        JOIN videos v ON bm.video_id = v.video_id WHERE bm.brand_id = ?
        GROUP BY v.channel_id ORDER BY cnt DESC LIMIT 1
    """, (brand_id,)).fetchone()

    # 3. Top Products (Fix for 0 mentions crash)
    top_products = conn.execute("""
        SELECT
            p.id,
            p.name,
            COUNT(pm.id) as cnt,
            COALESCE(AVG(pm.sentiment_score), 0) as score
        FROM products p
        LEFT JOIN product_mentions pm ON p.id = pm.product_id
        WHERE p.brand_id = ?
        GROUP BY p.id
        ORDER BY cnt DESC
    """, (brand_id,)).fetchall()

    # 4. Videos & Context
    videos_rows = conn.execute("""
        SELECT v.video_id, v.title, v.channel_name, v.upload_date, v.thumbnail_url,
               bm.mention_count, bm.sentiment_score
        FROM brand_mentions bm
        JOIN videos v ON bm.video_id = v.video_id
        WHERE bm.brand_id = ? ORDER BY v.upload_date DESC
    """, (brand_id,)).fetchall()

    videos = []
    llm_input = []
    for row in videos_rows:
        vid = dict(row)
        matches = conn.execute("SELECT text FROM video_segments WHERE video_id = ? AND lower(text) LIKE ? LIMIT 3", (vid['video_id'], f"%{brand['name'].lower()}%")).fetchall()
        snippet = " ... ".join([m['text'] for m in matches]) if matches else "Brand mentioned in video."

        vid['raw_snippet'] = snippet
        llm_input.append({"video_id": vid['video_id'], "date": vid['upload_date'], "text": snippet})
        videos.append(vid)

    # 5. Intelligence
    intelligence = get_brand_intelligence(conn, brand_id, brand['name'], llm_input, metrics['last_mentioned'])

    final_videos = []
    for v in videos:
        v['display_summary'] = intelligence.get('video_summaries', {}).get(v['video_id'], v['raw_snippet'])
        final_videos.append(v)

    # 6. Chart Data
    timeline = conn.execute("SELECT date(first_seen_date) as day, COUNT(*) as cnt, AVG(sentiment_score) as score FROM brand_mentions WHERE brand_id = ? GROUP BY day ORDER BY day ASC", (brand_id,)).fetchall()

    return render_template(
        "brand_profile.html",
        brand=brand,
        metrics=metrics,
        top_creator=top_creator,
        top_products=top_products,
        videos=final_videos,
        marketing_brief=intelligence.get('brief'),           # Text Brief
        marketing_brief_data=intelligence,                   # <--- CRITICAL: Pass full object for Word Cloud
        chart_labels=[r['day'] for r in timeline],
        chart_mentions=[r['cnt'] for r in timeline],
        chart_sentiment=[r['score'] for r in timeline]
    )


@app.route("/product/<int:product_id>")
def product_profile(product_id):
    conn = get_db()
    product = conn.execute("SELECT p.id, p.name, b.id AS brand_id, b.name AS brand_name FROM products p LEFT JOIN brands b ON p.brand_id = b.id WHERE p.id = ?", (product_id,)).fetchone()
    if not product: return "Product not found", 404

    metrics = conn.execute("SELECT COUNT(*) as total_mentions, COUNT(DISTINCT channel_id) as unique_channels, AVG(sentiment_score) as avg_sentiment, MAX(first_seen_date) as last_mentioned FROM product_mentions WHERE product_id = ?", (product_id,)).fetchone()
    top_creator = conn.execute("SELECT channel_name, COUNT(*) as cnt FROM product_mentions pm JOIN videos v ON pm.video_id = v.video_id WHERE pm.product_id = ? GROUP BY v.channel_id ORDER BY cnt DESC LIMIT 1", (product_id,)).fetchone()

    videos_rows = conn.execute("SELECT v.video_id, v.title, v.channel_name, v.upload_date, v.thumbnail_url, pm.mention_count, pm.sentiment_score FROM product_mentions pm JOIN videos v ON pm.video_id = v.video_id WHERE pm.product_id = ? ORDER BY v.upload_date DESC", (product_id,)).fetchall()

    videos = []
    llm_input = []
    for row in videos_rows:
        vid = dict(row)
        matches = conn.execute("SELECT text FROM video_segments WHERE video_id = ? AND lower(text) LIKE ? LIMIT 3", (vid['video_id'], f"%{product['name'].lower()}%")).fetchall()
        snippet = " ... ".join([m['text'] for m in matches]) if matches else "Mentioned in video."
        vid['raw_snippet'] = snippet
        llm_input.append({"video_id": vid['video_id'], "date": vid['upload_date'], "text": snippet})
        videos.append(vid)

    intelligence = get_product_intelligence(conn, product_id, product['name'], llm_input, metrics['last_mentioned'])
    final_videos = []
    for v in videos:
        v['display_summary'] = intelligence.get('video_summaries', {}).get(v['video_id'], v['raw_snippet'])
        final_videos.append(v)

    timeline = conn.execute("SELECT date(first_seen_date) as day, COUNT(*) as cnt, AVG(sentiment_score) as score FROM product_mentions WHERE product_id = ? GROUP BY day ORDER BY day ASC", (product_id,)).fetchall()

    return render_template("product_profile.html", product=product, metrics=metrics, top_creator=top_creator, videos=final_videos, marketing_brief_data=intelligence, chart_labels=[r['day'] for r in timeline], chart_mentions=[r['cnt'] for r in timeline], chart_sentiment=[r['score'] for r in timeline])

# ... (Keep existing video_profile, autocomplete, main) ...
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

@app.route("/brands/all")
def brands_directory():
    conn = get_db()

    # Sort by Mentions DESC, then Name
    # Also count distinct products per brand
    brands = conn.execute("""
        SELECT
            b.id,
            b.name,
            b.category,
            COUNT(DISTINCT bm.id) as mention_count,
            COUNT(DISTINCT p.id) as product_count
        FROM brands b
        LEFT JOIN brand_mentions bm ON b.id = bm.brand_id
        LEFT JOIN products p ON b.id = p.brand_id
        GROUP BY b.id
        ORDER BY mention_count DESC, b.name ASC
    """).fetchall()

    return render_template("brands_list.html", brands=brands)

@app.route("/products/all")
def products_directory():
    conn = get_db()

    # List products with Brand Name, ordered by mentions
    products = conn.execute("""
        SELECT
            p.id,
            p.name,
            b.name as brand_name,
            COUNT(pm.id) as mention_count
        FROM products p
        LEFT JOIN product_mentions pm ON p.id = pm.product_id
        LEFT JOIN brands b ON p.brand_id = b.id
        GROUP BY p.id
        ORDER BY mention_count DESC, p.name ASC
    """).fetchall()

    return render_template("products_list.html", products=products)

@app.route("/api/qa", methods=["POST"])
def api_qa():
    data = request.json
    context_type = data.get("context_type")
    context_name = data.get("context_name")
    question = data.get("question")

    # Aggregates & Segments passed from frontend (or we could fetch DB here)
    # For simplicity, we use what the frontend sends if available,
    # but strictly we should re-fetch for security.
    # Current implementation relies on the frontend passing the context it has.
    aggregates = data.get("aggregates", {})
    segments = data.get("segments", [])

    answer = ask_insights_llm(context_type, context_name, question, aggregates, segments)
    return jsonify({"answer": answer})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
