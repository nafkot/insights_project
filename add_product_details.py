import sqlite3
from config import DB_PATH

def upgrade_db():
    print(f"Upgrading database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Add new columns if they don't exist
    columns = [
        ("image_url", "TEXT"),
        ("main_category", "TEXT"),
        ("labels", "TEXT")
    ]

    for col_name, col_type in columns:
        try:
            c.execute(f"ALTER TABLE products ADD COLUMN {col_name} {col_type}")
            print(f" -> Added '{col_name}' column.")
        except sqlite3.OperationalError:
            print(f" -> '{col_name}' column already exists.")

    conn.commit()
    conn.close()
    print("Database upgrade complete.")

if __name__ == "__main__":
    upgrade_db()
