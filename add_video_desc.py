import sqlite3
from config import DB_PATH

def fix_videos_table():
    print(f"--- Fixing Schema for {DB_PATH} ---")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        c.execute("ALTER TABLE videos ADD COLUMN description TEXT")
        print("✅ Added 'description' column to videos table.")
    except sqlite3.OperationalError:
        print("ℹ️  'description' column already exists.")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    fix_videos_table()
