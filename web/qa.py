# web/qa.py
import os
import json
from typing import Any, Dict, List

from config import OPENAI_API_KEY, OPENAI_MODEL, GEMINI_API_KEY, GEMINI_MODEL

# OpenAI
try:
    from openai import OpenAI
    _openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except ImportError:
    _openai_client = None

# Gemini
try:
    import google.generativeai as genai
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        _gemini_model = genai.GenerativeModel(GEMINI_MODEL)
    else:
        _gemini_model = None
except ImportError:
    _gemini_model = None


def build_insights_prompt(
    context_type: str,
    context_name: str,
    question: str,
    aggregates: Dict[str, Any],
    segments: List[Dict[str, Any]],
) -> str:
    agg_json = json.dumps(aggregates, ensure_ascii=False, indent=2)

    seg_snippets = []
    for s in segments[:30]:
        seg_snippets.append(
            f"- [{s.get('upload_date','')}] (video {s.get('video_id')}) "
            f"{s.get('text','')[:400]}"
        )
    seg_block = "\n".join(seg_snippets)

    prompt = f"""
You are a careful insights analyst for social video data.

CONTEXT TYPE: {context_type.upper()}
ENTITY NAME: {context_name}

USER QUESTION:
\"\"\"{question}\"\"\"


AGGREGATED STATS (JSON):
{agg_json}

TEXT SEGMENTS (snippets from relevant videos):
{seg_block}

INSTRUCTIONS:
- Use ONLY the data above to answer.
- Be concise and practical (3–6 sentences).
- Refer to numbers and trends when possible.
- If something is not in the data, say you cannot be sure.
- Do NOT hallucinate or invent metrics.

Now answer the user's question:
"""
    return prompt.strip()


def call_openai(prompt: str) -> str | None:
    if not _openai_client:
        return None
    try:
        resp = _openai_client.responses.create(
            model=OPENAI_MODEL,
            input=prompt,
        )
        return resp.output[0].content[0].text.strip()
    except Exception as e:
        print(f"[QA] OpenAI error: {e}")
        return None


def call_gemini(prompt: str) -> str | None:
    if not _gemini_model:
        return None
    try:
        resp = _gemini_model.generate_content(prompt)
        return (resp.text or "").strip()
    except Exception as e:
        print(f"[QA] Gemini error: {e}")
        return None


def ask_insights_llm(
    context_type: str,
    context_name: str,
    question: str,
    aggregates: Dict[str, Any],
    segments: List[Dict[str, Any]],
) -> str:
    prompt = build_insights_prompt(context_type, context_name, question, aggregates, segments)

    answer = call_openai(prompt)
    if answer:
        return answer

    answer = call_gemini(prompt)
    if answer:
        return answer

    return "I'm unable to generate an answer right now because no language model is configured or reachable."

