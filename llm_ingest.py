# llm_ingest.py
import json
from typing import Dict, Any
from config import OPENAI_API_KEY, OPENAI_MODEL, GEMINI_API_KEY, GEMINI_MODEL

# Try set up OpenAI client
try:
    from openai import OpenAI
    openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except ImportError:
    openai_client = None

# Try set up Gemini client
try:
    import google.generativeai as genai
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel(GEMINI_MODEL)
    else:
        gemini_model = None
except ImportError:
    gemini_model = None


INGESTION_PROMPT_TEMPLATE = """
You are a highly accurate video transcript analysis engine.

Your task is to extract structured, factual information from the transcript.
Work ONLY with what is explicitly stated. DO NOT guess or infer anything.

---------------------------------------------
RETURN YOUR ANSWER AS STRICT JSON USING
THE EXACT SCHEMA BELOW.
---------------------------------------------

{{
  "summary": "",
  "sentiment": "Positive | Neutral | Negative",
  "topics": [],
  "brands": [],
  "products": [],
  "sponsors": []
}}

---------------------------------------------
DETAILED INSTRUCTIONS
---------------------------------------------

GENERAL RULES:
- DO NOT hallucinate.
- DO NOT invent brands, products, or sponsors.
- ONLY output what is present in the transcript.
- If you are unsure, leave the field empty.
- If no items are found for a field, return an empty list [].

SUMMARY:
- Provide a short, factual, neutral summary of the transcript.

SENTIMENT:
Choose ONE of:
- Positive  (enthusiastic, praising, recommending)
- Neutral   (informational, mixed, no strong stance)
- Negative  (complaints, issues, dissatisfaction)

TOPICS:
- Extract general discussion themes.
- Example topics: "makeup tutorial", "product review", "skincare", "vlog", "tech analysis", etc.

BRANDS:
- Return ONLY real brands explicitly mentioned.
- Include names exactly as spoken.
- Examples: "Maybelline", "NARS", "Fenty Beauty", "Samsung".
- DO NOT infer a brand from a product unless explicitly stated.

PRODUCTS:
- Return product names mentioned in the transcript.
- Use simple names (e.g., "Fit Me Foundation", "Sky High Mascara").
- DO NOT fabricate products.
- If the transcript says only “Fit Me,” mapping it to "Fit Me Foundation" is allowed.

SPONSORS:
- Sponsors are entities in phrases like:
  - “This video is sponsored by…”
  - “Thanks to X for sponsoring this video…”
  - “In partnership with…”
- Return sponsor names ONLY if explicitly stated.

---------------------------------------------
CONTEXTUAL DATA AVAILABLE:
VIDEO TITLE: {title}
CHANNEL NAME: {channel}

TRANSCRIPT:
{text}

---------------------------------------------
RETURN STRICT JSON ONLY.
No Markdown, no commentary, no text outside the JSON block.
---------------------------------------------
"""


def _call_openai(prompt: str) -> str | None:
    if not openai_client:
        return None
    try:
        resp = openai_client.responses.create(
            model=OPENAI_MODEL,
            input=prompt,
        )
        return resp.output[0].content[0].text.strip()
    except Exception as e:
        print(f"[LLM_INGEST] OpenAI error: {e}")
        return None


def _call_gemini(prompt: str) -> str | None:
    if not gemini_model:
        return None
    try:
        resp = gemini_model.generate_content(prompt)
        return (resp.text or "").strip()
    except Exception as e:
        print(f"[LLM_INGEST] Gemini error: {e}")
        return None


def _safe_parse_json(s: str) -> Dict[str, Any]:
    # Try to extract a JSON object even if model adds extra text
    s = s.strip()
    first = s.find("{")
    last = s.rfind("}")
    if first != -1 and last != -1 and last > first:
        s = s[first:last+1]
    try:
        return json.loads(s)
    except Exception:
        return {}


def analyze_transcript(title: str, channel: str, text: str) -> Dict[str, Any]:
    """
    High-level ingestion call:
    - Builds prompt
    - Calls OpenAI then Gemini
    - Ensures a safe structured dict
    """
    if not text.strip():
        return {
            "summary": "",
            "sentiment": "Neutral",
            "topics": [],
            "brands": [],
            "products": [],
            "sponsors": [],
        }

    prompt = INGESTION_PROMPT_TEMPLATE.format(
        title=title or "",
        channel=channel or "",
        text=text[:12000],  # keep within sane context for now
    )

    raw = _call_openai(prompt) or _call_gemini(prompt)
    if not raw:
        return {
            "summary": "",
            "sentiment": "Neutral",
            "topics": [],
            "brands": [],
            "products": [],
            "sponsors": [],
        }

    data = _safe_parse_json(raw)
    # Normalise fields
    return {
        "summary": data.get("summary", "") or "",
        "sentiment": data.get("sentiment", "Neutral") or "Neutral",
        "topics": data.get("topics", []) or [],
        "brands": data.get("brands", []) or [],
        "products": data.get("products", []) or [],
        "sponsors": data.get("sponsors", []) or [],
    }

