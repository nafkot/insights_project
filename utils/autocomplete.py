import sqlite3
import json
from config import DB_PATH
from openai import OpenAI

client = OpenAI()

def llm_semantic_suggestions(term: str):
    """
    LLM fallback: given a short user input, predict likely
    brands / products / sponsors / channel-style terms.

    Returns a simple list of suggestion strings.
    """
    prompt = f"""
User typed this partial search term: "{term}".

You are an autocomplete engine for a creator-marketing insights tool
that tracks YouTube / social media channels, brands, products and sponsors.

Task:
- Predict up to 5 likely full search phrases the user might mean.
- Focus on realistic beauty/brand/product/sponsor queries or channel-style names.
- Return ONLY a JSON list of strings, no extra text.

Example output:
["maybelline", "maybelline fit me foundation", "sephora haul", "tati westbrook", "rare beauty blush"]
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        # Try to parse JSON; if it fails, just return empty
        suggestions = json.loads(raw)
        if isinstance(suggestions, list):
            return [str(s).strip() for s in suggestions if str(s).strip()]
        return []
    except Exception as e:
        print("[LLM AUTOCOMPLETE ERROR]", e)
        return []

def db_autocomplete_search(term):
    """
    Pure SQL autocomplete across channels, brands, products, sponsors.
    Returns up to 10 total suggestions.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    term_like = f"{term.lower()}%"

    results = {
        "channels": [],
        "brands": [],
        "products": [],
        "sponsors": []
    }

    # Channels
    rows = c.execute("""
        SELECT DISTINCT channel_name
        FROM videos
        WHERE LOWER(channel_name) LIKE ?
        LIMIT 10
    """, (term_like,)).fetchall()
    results["channels"] = [r[0] for r in rows]

    # Brands
    rows = c.execute("""
        SELECT DISTINCT brand_name
        FROM brands
        WHERE LOWER(brand_name) LIKE ?
        LIMIT 10
    """, (term_like,)).fetchall()
    results["brands"] = [r[0] for r in rows]

    # Products
    rows = c.execute("""
        SELECT DISTINCT product_name
        FROM products
        WHERE LOWER(product_name) LIKE ?
        LIMIT 10
    """, (term_like,)).fetchall()
    results["products"] = [r[0] for r in rows]

    # Sponsors
    rows = c.execute("""
        SELECT DISTINCT sponsor_name
        FROM sponsors
        WHERE LOWER(sponsor_name) LIKE ?
        LIMIT 10
    """, (term_like,)).fetchall()
    results["sponsors"] = [r[0] for r in rows]

    conn.close()
    return results


def llm_autocomplete_fallback(term):
    """
    LLM predicts intended search terms if DB results are weak.
    """
    prompt = f"""
User typed: "{term}"

You are an autocomplete engine for a social-media insight tool.
Predict what they most likely meant, considering:
- popular brands (Maybelline, Sephora, Elf, Tarte, Rare Beauty)
- common product lines (Fit Me, Luminous Silk, Shape Tape)
- influencer channels (Tati, Allie Glines, Nikkie Tutorials)
- sponsor names (NordVPN, Honey, Squarespace)

Return a JSON list of 5 suggestions.
Example: ["maybelline", "maybelline fit me", "fit me foundation"]
"""

    try:
        res = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.1,
            messages=[{"role":"user","content": prompt}]
        )
        text = res.choices[0].message["content"]
        return json.loads(text)
    except Exception as e:
        return []
    

def hybrid_autocomplete(term):
    """
    Mode B (Hybrid):
    1) Run DB autocomplete
    2) If weak (<3 results total) → LLM fallback predictions
    """
    db_results = db_autocomplete_search(term)

    # Count total DB matched items
    total = sum(len(v) for v in db_results.values())

    if total >= 3:
        return db_results  # strong enough

    # Otherwise fallback to LLM
    llm_preds = llm_autocomplete_fallback(term)

    db_results["semantic"] = llm_preds
    return db_results

