import sqlite3
from config import DB_PATH

def add_missing_columns():
    print(f"Updating schema for {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Columns to add
    new_columns = [
        ("image_url", "TEXT"),
        ("main_category", "TEXT"),
        ("labels", "TEXT")
    ]

    for col_name, col_type in new_columns:
        try:
            c.execute(f"ALTER TABLE products ADD COLUMN {col_name} {col_type}")
            print(f"✅ Added '{col_name}' column.")
        except sqlite3.OperationalError:
            print(f"ℹ️  '{col_name}' column already exists.")

    conn.commit()
    conn.close()
    print("Database schema updated.")

if __name__ == "__main__":
    add_missing_columns()
