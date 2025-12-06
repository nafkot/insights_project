import sqlite3
from config import DB_PATH

def add_platform():
    print(f"Updating database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 1. Add platform column to channels
    try:
        c.execute("ALTER TABLE channels ADD COLUMN platform TEXT DEFAULT 'YouTube'")
        print(" -> Added 'platform' column to channels table.")
    except sqlite3.OperationalError:
        print(" -> 'platform' column already exists in channels.")

    # 2. Add avatar_url column to channels (helpful for UI)
    try:
        c.execute("ALTER TABLE channels ADD COLUMN avatar_url TEXT")
        print(" -> Added 'avatar_url' column to channels table.")
    except sqlite3.OperationalError:
        print(" -> 'avatar_url' column already exists.")

    conn.commit()
    conn.close()
    print("Database schema updated.")

if __name__ == "__main__":
    add_platform()
