import sqlite3
from config import DB_PATH

def add_social_columns():
    print(f"Updating schema for {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Define new columns
    social_cols = [
        ("email", "TEXT"),
        ("website", "TEXT"),
        ("instagram", "TEXT"),
        ("tiktok", "TEXT"),
        ("twitter", "TEXT"),
        ("spotify", "TEXT"),
        ("soundcloud", "TEXT")
    ]

    for col, type_ in social_cols:
        try:
            c.execute(f"ALTER TABLE channels ADD COLUMN {col} {type_}")
            print(f"✅ Added '{col}' column.")
        except sqlite3.OperationalError:
            print(f"ℹ️  '{col}' column already exists.")

    conn.commit()
    conn.close()
    print("Database schema updated.")

if __name__ == "__main__":
    add_social_columns()
