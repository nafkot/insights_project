from openai import OpenAI
import sqlite3
import json
import os
from config import DB_PATH, OPENAI_API_KEY

client = OpenAI(api_key=OPENAI_API_KEY)

def answer_user_query(query: str, channel_ids=None):
    """
    LLM-powered semantic search over transcripts + metadata.
    """
    if not query or len(query.strip()) == 0:
        return "Please enter a search query."

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 1. Search Videos
    sql_videos = """
        SELECT video_id, title, channel_name, overall_summary
        FROM videos
        WHERE title LIKE ? OR overall_summary LIKE ?
        ORDER BY upload_date DESC LIMIT 10
    """
    q_like = f"%{query}%"
    videos = c.execute(sql_videos, (q_like, q_like)).fetchall()

    # 2. Search Transcript Segments
    sql_segments = """
        SELECT video_id, text, start_time
        FROM video_segments
        WHERE text LIKE ?
        LIMIT 10
    """
    segments = c.execute(sql_segments, (q_like,)).fetchall()

    conn.close()

    if not videos and not segments:
        return None

    # 3. Context Builder
    context_str = "VIDEOS FOUND:\n"
    for v in videos[:5]:
        context_str += f"- {v['title']} (Channel: {v['channel_name']}): {v['overall_summary']}\n"

    context_str += "\nTRANSCRIPT MATCHES:\n"
    for s in segments[:5]:
        context_str += f"- ...{s['text']}...\n"

    # 4. LLM Call
    prompt = f"""
    User Query: "{query}"

    Based on the following database matches, provide a 2-sentence summary answering the user's intent.
    If the user is asking about a person/channel (e.g. "Tati"), summarize who they are based on the context.

    Context:
    {context_str}
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "You are a helpful search assistant."},
                      {"role": "user", "content": prompt}],
            temperature=0.3,
        )
        # FIX: Use dot notation instead of brackets
        return response.choices[0].message.content.strip()

    except Exception as e:
        print(f"LLM Error: {e}")
        return None
