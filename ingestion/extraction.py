# ingestion/extraction.py

import sqlite3
import hashlib
import json
import concurrent.futures
import time
import random
from typing import List, Dict, Tuple, Optional

from openai import OpenAI, RateLimitError, APIError
from config import DB_PATH, OPENAI_API_KEY, OPENAI_MODEL

client = OpenAI(api_key=OPENAI_API_KEY)

# --- STRICT PROMPT (MERGED: Rules + Summary) ---
SYSTEM_PROMPT = """
You are a detailed commercial text extraction engine.
Your goal is to extract Brands, Specific Products, Categories, Topics, and a Summary.

RULES FOR PRODUCTS:
1. **Specific Names Only**: The "product" field must be the specific sub-brand or line name (e.g., "Diorshow", "Fix Plus", "Shape Tape", "Double Wear").
2. **No Generics**: Do NOT use generic terms (e.g., "mascara", "lipstick", "foundation", "nail polish") as the product name.
   - INCORRECT: { "brand": "Dior", "product": "mascara" }
   - CORRECT:   { "brand": "Dior", "product": null, "category": "Mascara" }
3. **Context**: Always try to infer the "category" (e.g., Mascara, Setting Spray, Foundation) even if the product name is specific.

RULES FOR TOPICS:
1. Extract general themes (e.g., "Skincare Routine", "Travel Vlog", "Makeup Tutorial", "Unboxing", "Haul", "Review").
2. Keep them concise (2-3 words max).
3. Do not include specific brand names as topics.

RULES FOR SUMMARY:
1. Provide a concise, 1-sentence summary of the main discussion in this segment.

RETURN JSON SCHEMA:
{
  "summary": "One sentence summary of the content.",
  "brands": ["Brand1", "Brand2"],
  "products": [
    {
      "brand": "MAC",
      "product": "Fix Plus",
      "category": "Setting Spray"
    }
  ],
  "topics": ["Topic1", "Topic2"],
  "sponsors": ["Sponsor1"]
}
"""

def _get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def ensure_extraction_cache_table(conn: sqlite3.Connection) -> None:
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS video_extraction_cache (
            video_id TEXT PRIMARY KEY,
            transcript_hash TEXT,
            brands_json TEXT,
            products_json TEXT,
            sponsors_json TEXT,
            topics_json TEXT,
            summary TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Migrations for existing tables
    try:
        c.execute("ALTER TABLE video_extraction_cache ADD COLUMN topics_json TEXT")
    except sqlite3.OperationalError: pass

    try:
        c.execute("ALTER TABLE video_extraction_cache ADD COLUMN summary TEXT")
    except sqlite3.OperationalError: pass

    conn.commit()

def compute_transcript_hash(segments: List[Dict]) -> str:
    lines = []
    for seg in segments or []:
        text = (seg.get("text") or "").strip()
        lines.append(f"{seg.get('start',0)}:{seg.get('end',0)}:{text}")
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()

def get_cached_extraction(conn: sqlite3.Connection, video_id: str, transcript_hash: str):
    c = conn.cursor()
    row = c.execute("SELECT transcript_hash, brands_json, products_json, sponsors_json, topics_json, summary FROM video_extraction_cache WHERE video_id = ?", (video_id,)).fetchone()
    if not row or row[0] != transcript_hash: return None
    try:
        topics = json.loads(row[4]) if row[4] else []
        summary = row[5] if row[5] else ""
        return json.loads(row[1]), json.loads(row[2]), json.loads(row[3]), topics, summary
    except: return None

def save_extraction_cache(conn: sqlite3.Connection, video_id: str, transcript_hash: str, brands, products, sponsors, topics, summary):
    conn.execute("""
        INSERT INTO video_extraction_cache (video_id, transcript_hash, brands_json, products_json, sponsors_json, topics_json, summary, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(video_id) DO UPDATE SET
            transcript_hash=excluded.transcript_hash,
            brands_json=excluded.brands_json,
            products_json=excluded.products_json,
            sponsors_json=excluded.sponsors_json,
            topics_json=excluded.topics_json,
            summary=excluded.summary,
            updated_at=CURRENT_TIMESTAMP
    """, (video_id, transcript_hash, json.dumps(brands), json.dumps(products), json.dumps(sponsors), json.dumps(topics), summary))
    conn.commit()

def _chunk_text(text: str, max_chars: int = 12000) -> List[str]:
    text = text.strip()
    if len(text) <= max_chars: return [text]

    chunks = []
    while text:
        if len(text) <= max_chars:
            chunks.append(text)
            break
        cut = text.rfind('\n', 0, max_chars)
        if cut == -1: cut = text.rfind(' ', 0, max_chars)
        if cut == -1: cut = max_chars
        chunks.append(text[:cut])
        text = text[cut:].strip()
    return chunks

def _call_llm_for_entities(text: str, max_retries=5) -> Dict:
    if not text.strip(): return {"brands":[], "products":[], "sponsors":[], "topics":[], "summary": ""}

    delay = 2

    for attempt in range(max_retries):
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL, temperature=0, top_p=1,
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": SYSTEM_PROMPT},
                          {"role": "user", "content": f"Extract entities:\n{text}"}]
            )
            return json.loads(resp.choices[0].message.content)

        except RateLimitError:
            if attempt < max_retries - 1:
                sleep_time = delay + random.uniform(0, 1)
                print(f"[Rate Limit] Waiting {sleep_time:.1f}s before retry {attempt+1}/{max_retries}...")
                time.sleep(sleep_time)
                delay *= 2
            else:
                print(f"[Extraction Error] Rate limit exceeded after {max_retries} retries.")
                return {"brands":[], "products":[], "sponsors":[], "topics":[], "summary": ""}

        except Exception as e:
            print(f"[Chunk Error] {e}")
            return {"brands":[], "products":[], "sponsors":[], "topics":[], "summary": ""}

    return {"brands":[], "products":[], "sponsors":[], "topics":[], "summary": ""}

def extract_entities_for_video(video_id: str, segments: List[Dict]) -> Tuple[List[str], List[Dict], List[str], List[str], str]:
    segments = sorted(segments or [], key=lambda s: s.get("start", 0.0))
    transcript_hash = compute_transcript_hash(segments)

    conn = _get_conn()
    try:
        ensure_extraction_cache_table(conn)
        cached = get_cached_extraction(conn, video_id, transcript_hash)
        if cached:
            print(f"[{video_id}] Using cached entities.")
            return cached

        full_text = "\n".join((s.get("text") or "").strip() for s in segments)
        chunks = _chunk_text(full_text)

        print(f"[{video_id}] Processing {len(chunks)} chunks...")

        agg_brands, agg_products, agg_sponsors, agg_topics = [], [], [], []
        first_summary = ""

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(_call_llm_for_entities, chunks))

        for i, res in enumerate(results):
            agg_brands.extend(res.get("brands", []))
            agg_products.extend(res.get("products", []))
            agg_sponsors.extend(res.get("sponsors", []))
            agg_topics.extend(res.get("topics", []))

            # Use the summary from the first chunk (intro) as the main video summary
            if i == 0 and res.get("summary"):
                first_summary = res.get("summary")

        # Dedupe
        brand_set = sorted({b.strip() for b in agg_brands if b})
        sponsor_set = sorted({s.strip() for s in agg_sponsors if s})
        topic_set = sorted({t.strip().title() for t in agg_topics if t})

        product_map = {}
        for p in agg_products:
            if isinstance(p, dict):
                p_name = (p.get("product") or "").strip()
                b_name = (p.get("brand") or "").strip()
                cat = (p.get("category") or "").strip()

                key = f"{b_name}::{p_name}"
                if key not in product_map:
                    product_map[key] = {
                        "brand": b_name if b_name else None,
                        "product": p_name if p_name else None,
                        "category": cat if cat else None
                    }

        products_out = list(product_map.values())

        # Save all 5 items to cache
        save_extraction_cache(conn, video_id, transcript_hash, brand_set, products_out, sponsor_set, topic_set, first_summary)

        real_products = [p for p in products_out if p['product']]
        print(f"[{video_id}] Extraction complete: {len(brand_set)} brands, {len(real_products)} products, {len(topic_set)} topics.")

        return brand_set, products_out, sponsor_set, topic_set, first_summary
    finally:
        conn.close()
