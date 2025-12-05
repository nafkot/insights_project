from openai import OpenAI
import sqlite3
import json
import re
from config import DB_PATH

client = OpenAI()

def answer_user_query(query: str, channel_ids=None):
    """
    LLM-powered semantic search over transcripts + metadata.

    Steps:
      1) DB HYBRID SEARCH:
         - find relevant videos by TITLE
         - find relevant videos by SUMMARY
         - find relevant transcript SEGMENTS
         - limit to channels if specified

      2) CONTEXT BUILDER:
         - assemble a compact knowledge pack for the LLM
         - includes: titles, summaries, transcript excerpts,
                     brand/product mentions, sentiment signals

      3) LLM REASONING:
         - produce final answer explaining trends, insights,
           and referencing which channels/videos the answer came from.
    """

    if not query or len(query.strip()) == 0:
        return "Please enter a search query."

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # ---------------------------------------
    # 1) BUILD SQL FILTERS
    # ---------------------------------------
    channel_filter = ""
    params = []

    if channel_ids:
        placeholders = ",".join("?" for _ in channel_ids)
        channel_filter = f" AND channel_id IN ({placeholders}) "
        params.extend(channel_ids)

    query_like = f"%{query.lower()}%"

    # ---------------------------------------
    # 2) FIND MATCHING VIDEOS
    # ---------------------------------------
    sql_videos = f"""
        SELECT video_id, title, channel_name, overall_summary, topics
        FROM videos
        WHERE (LOWER(title) LIKE ? OR LOWER(overall_summary) LIKE ?)
        {channel_filter}
        ORDER BY upload_date DESC
        LIMIT 20
    """

    rows = c.execute(sql_videos, (query_like, query_like, *params)).fetchall()

    video_context = []

    for vid, title, ch, summary, topics in rows:
        video_context.append({
            "video_id": vid,
            "title": title,
            "channel": ch,
            "summary": summary,
            "topics": (topics or "").split(",") if topics else []
        })

    # ---------------------------------------
    # 3) MATCHING TRANSCRIPT SEGMENTS
    # ---------------------------------------
    sql_segments = f"""
        SELECT video_id, start_time, end_time, text
        FROM video_segments
        WHERE LOWER(text) LIKE ?
        {channel_filter}
        LIMIT 30
    """

    seg_rows = c.execute(sql_segments, (query_like, *params)).fetchall()

    segment_context = [
        {
            "video_id": vid,
            "start": start,
            "end": end,
            "text": txt
        }
        for vid, start, end, txt in seg_rows
    ]

    conn.close()

    # ---------------------------------------
    # 4) IF NOTHING FOUND → Return politely
    # ---------------------------------------
    if not video_context and not segment_context:
        return f"No content found related to '{query}'. Try another phrase."

    # ---------------------------------------
    # 5) PREP LLM CONTEXT PACK
    # ---------------------------------------
    llm_payload = {
        "query": query,
        "videos": video_context[:12],         # keep compact
        "transcript_matches": segment_context[:20],  # keep compact
    }

    # ---------------------------------------
    # 6) CALL LLM — GPT-4.1-mini or GPT-4.1
    # ---------------------------------------
    prompt = f"""
You are an analytics engine for a social-media insight tool.

USER QUERY:
"{query}"

DATABASE CONTEXT (JSON BELOW):
{json.dumps(llm_payload, indent=2)}

TASK:
1. Understand what the user is searching for.
2. Analyse the provided video summaries, topics, and transcript excerpts.
3. Produce a structured explanation answering the user's question.
4. Highlight:
    - Key trends
    - Repeated themes
    - Mentioned brands/products (if any)
    - Sentiment patterns
5. Reference which channels/videos your answer is based on.
6. Keep the output concise but insightful.

Return plain text. No JSON.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "system", "content": "You are an expert insights analyst."},
                      {"role": "user", "content": prompt}],
            temperature=0.2,
        )
        return response.choices[0].message["content"].strip()

    except Exception as e:
        return f"(Error calling LLM) {str(e)}"

