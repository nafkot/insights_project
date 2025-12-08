from ingest_video import ingest_single_video
import sqlite3

# 1. Run Ingestion
print("Running ingestion for fWfrkV6pu14...")
ingest_single_video("fWfrkV6pu14")

# 2. Check Result immediately
conn = sqlite3.connect('youtube_insights.db')
row = conn.execute("SELECT overall_summary, topics FROM videos WHERE video_id = 'fWfrkV6pu14'").fetchone()
print("\n--- DATABASE RESULT ---")
print(f"Summary: {row[0]}")
print(f"Topics:  {row[1]}")
conn.close()
