import sqlite3
import os
from config import DB_PATH

def fix_schema():
    print(f"Checking database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 1. Fix Products Table (Add brand_id)
    print("Checking 'products' table for 'brand_id'...")
    try:
        c.execute("SELECT brand_id FROM products LIMIT 1")
        print(" -> 'brand_id' already exists.")
    except sqlite3.OperationalError:
        print(" -> 'brand_id' missing. Adding it now...")
        c.execute("ALTER TABLE products ADD COLUMN brand_id INTEGER REFERENCES brands(id)")
        conn.commit()
        print(" -> Done.")

    conn.close()

if __name__ == "__main__":
    fix_schema()
