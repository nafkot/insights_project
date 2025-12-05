# ingestion/extraction.py

import sqlite3
import hashlib
import json
from typing import List, Dict, Tuple, Optional

from openai import OpenAI

from config import DB_PATH, OPENAI_API_KEY, OPENAI_MODEL

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = """
You are a strict text extraction engine.
You DO NOT infer, guess, expand, correct, or interpret meaning.
You only detect explicit brand or product names EXACTLY as written in the text.

Rules:
- Do NOT hallucinate or invent things that are not in the text.
- Do NOT merge, split, or normalise names beyond trimming whitespace.
- Do NOT infer brands from context.
- Only include names that appear literally in the text.
- Preserve casing as seen in the text.
- If something appears multiple times, list it once.
- If a product is mentioned without a clear brand, set brand to null.

Return ONLY valid JSON matching this schema:

{
  "brands": ["Brand1", "Brand2"],
  "products": [
    { "brand": "Brand1", "product": "ProductA" },
    { "brand": null,    "product": "ProductB" }
  ],
  "sponsors": ["Sponsor1", "Sponsor2"]
}
"""


def _get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def ensure_extraction_cache_table(conn: sqlite3.Connection) -> None:
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS video_extraction_cache (
            video_id TEXT PRIMARY KEY,
            transcript_hash TEXT,
            brands_json TEXT,
            products_json TEXT,
            sponsors_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def compute_transcript_hash(segments: List[Dict]) -> str:
    """
    Option A: hash over start:end:text lines.
    This will be stable as long as segments don't change.
    """
    lines = []
    for seg in segments or []:
        start = seg.get("start", 0)
        end = seg.get("end", 0)
        text = (seg.get("text") or "").strip()
        lines.append(f"{start}:{end}:{text}")
    payload = "\n".join(lines)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_cached_extraction(
    conn: sqlite3.Connection, video_id: str, transcript_hash: str
) -> Optional[Tuple[List[str], List[Dict], List[str]]]:
    """
    If we have a cache row AND the transcript hash matches, return cached entities.
    """
    c = conn.cursor()
    c.execute(
        """
        SELECT transcript_hash, brands_json, products_json, sponsors_json
        FROM video_extraction_cache
        WHERE video_id = ?
        """,
        (video_id,),
    )
    row = c.fetchone()
    if not row:
        return None

    cached_hash, brands_json, products_json, sponsors_json = row
    if cached_hash != transcript_hash:
        # Transcript changed – ignore cache
        return None

    try:
        brands = json.loads(brands_json) if brands_json else []
        products = json.loads(products_json) if products_json else []
        sponsors = json.loads(sponsors_json) if sponsors_json else []
        return brands, products, sponsors
    except Exception as e:
        print(f"[{video_id}] Failed to parse cached extraction JSON: {e}")
        return None


def save_extraction_cache(
    conn: sqlite3.Connection,
    video_id: str,
    transcript_hash: str,
    brands: List[str],
    products: List[Dict],
    sponsors: List[str],
) -> None:
    c = conn.cursor()
    brands_json = json.dumps(brands)
    products_json = json.dumps(products)
    sponsors_json = json.dumps(sponsors)

    c.execute(
        """
        INSERT INTO video_extraction_cache (
            video_id, transcript_hash, brands_json, products_json, sponsors_json,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(video_id) DO UPDATE SET
            transcript_hash = excluded.transcript_hash,
            brands_json = excluded.brands_json,
            products_json = excluded.products_json,
            sponsors_json = excluded.sponsors_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (video_id, transcript_hash, brands_json, products_json, sponsors_json),
    )
    conn.commit()


def _call_llm_for_entities(text: str) -> Dict:
    """
    Single deterministic LLM call to extract brands/products/sponsors from text.
    """
    if not text.strip():
        return {"brands": [], "products": [], "sponsors": []}

    user_prompt = f"""
Extract brands, products, and sponsors explicitly mentioned in the following transcript text.

TEXT:
{text}

Remember:
- Only include names that appear literally in the text.
- Do NOT guess or invent anything.
- If a product has no clear brand, set "brand": null.
Return ONLY valid JSON with this schema:

{{
  "brands": ["Brand1", "Brand2"],
  "products": [
    {{"brand": "Brand1", "product": "ProductA"}},
    {{"brand": null,    "product": "ProductB"}}
  ],
  "sponsors": ["Sponsor1", "Sponsor2"]
}}
"""

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0,
        top_p=1,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT.strip()},
            {"role": "user", "content": user_prompt.strip()},
        ],
    )

    # openai>=1.0: parsed JSON is available
    try:
        data = resp.choices[0].message.parsed  # type: ignore[attr-defined]
    except AttributeError:
        # Fallback: parse content manually if 'parsed' is not available
        content = resp.choices[0].message.content
        data = json.loads(content)

    # Normalise keys
    brands = data.get("brands") or []
    products = data.get("products") or []
    sponsors = data.get("sponsors") or []

    # Basic type safety
    if not isinstance(brands, list):
        brands = []
    if not isinstance(products, list):
        products = []
    if not isinstance(sponsors, list):
        sponsors = []

    return {
        "brands": brands,
        "products": products,
        "sponsors": sponsors,
    }


def _normalise_and_dedupe(
    brands: List[str], products: List[Dict], sponsors: List[str]
) -> Tuple[List[str], List[Dict], List[str]]:
    # Normalise + dedupe brands
    brand_set = set()
    for b in brands:
        if not b:
            continue
        b_norm = b.strip()
        if b_norm:
            brand_set.add(b_norm)
    brands_out = sorted(brand_set)

    # Normalise + dedupe sponsors
    sponsor_set = set()
    for s in sponsors:
        if not s:
            continue
        s_norm = s.strip()
        if s_norm:
            sponsor_set.add(s_norm)
    sponsors_out = sorted(sponsor_set)

    # Normalise + dedupe products
    product_set = set()
    products_out = []
    for p in products:
        if not isinstance(p, dict):
            continue
        brand = p.get("brand")
        product = p.get("product")
        if not product:
            continue
        brand_norm = brand.strip() if isinstance(brand, str) else None
        product_norm = product.strip()
        key = (brand_norm, product_norm)
        if key in product_set:
            continue
        product_set.add(key)
        products_out.append({"brand": brand_norm, "product": product_norm})

    return brands_out, products_out, sponsors_out


def extract_entities_for_video(
    video_id: str, segments: List[Dict]
) -> Tuple[List[str], List[Dict], List[str]]:
    """
    Main entry point used by ingest_video.py

    - Computes a stable hash of the transcript segments.
    - Checks cache; if hash matches, returns cached entities.
    - Otherwise, calls LLM once over the full text, normalises + dedupes,
      saves cache, and returns.
    """
    # Ensure segments have stable order
    segments = sorted(segments or [], key=lambda s: s.get("start", 0.0))

    transcript_hash = compute_transcript_hash(segments)

    # Concatenate text for a single LLM call
    full_text = "\n".join((seg.get("text") or "").strip() for seg in segments)

    conn = _get_conn()
    try:
        ensure_extraction_cache_table(conn)

        cached = get_cached_extraction(conn, video_id, transcript_hash)
        if cached is not None:
            brands, products, sponsors = cached
            print(
                f"[{video_id}] Using cached brand/product extraction "
                f"(brands={len(brands)}, products={len(products)}, sponsors={len(sponsors)})"
            )
            return brands, products, sponsors

        # No valid cache: call LLM once
        print(f"[{video_id}] Calling LLM for deterministic brand/product extraction...")
        raw = _call_llm_for_entities(full_text)
        brands_raw = raw.get("brands") or []
        products_raw = raw.get("products") or []
        sponsors_raw = raw.get("sponsors") or []

        brands, products, sponsors = _normalise_and_dedupe(
            brands_raw, products_raw, sponsors_raw
        )

        save_extraction_cache(conn, video_id, transcript_hash, brands, products, sponsors)

        print(
            f"[{video_id}] Extracted entities: brands={len(brands)}, "
            f"products={len(products)}, sponsors={len(sponsors)}"
        )
        return brands, products, sponsors
    finally:
        conn.close()

