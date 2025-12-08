import sqlite3
from config import DB_PATH

def fix_videos_table():
    print(f"--- Fixing Schema for {DB_PATH} ---")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 1. Add comment_count
    try:
        c.execute("ALTER TABLE videos ADD COLUMN comment_count INTEGER DEFAULT 0")
        print("✅ Added 'comment_count' column.")
    except sqlite3.OperationalError:
        print("ℹ️  'comment_count' already exists.")

    # 2. Add topics (just in case)
    try:
        c.execute("ALTER TABLE videos ADD COLUMN topics TEXT")
        print("✅ Added 'topics' column.")
    except sqlite3.OperationalError:
        print("ℹ️  'topics' already exists.")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    fix_videos_table()
