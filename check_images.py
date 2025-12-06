import sqlite3
from config import DB_PATH

def check_images():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    print(f"--- Checking Product Images in {DB_PATH} ---")
    
    # 1. Count how many products have images
    total = c.execute("SELECT count(*) FROM products").fetchone()[0]
    with_img = c.execute("SELECT count(*) FROM products WHERE image_url IS NOT NULL AND image_url != ''").fetchone()[0]
    
    print(f"Total Products: {total}")
    print(f"Products with Images: {with_img} ({with_img/total*100:.1f}%)")
    print("-" * 40)

    # 2. Show first 10 examples
    rows = c.execute("SELECT id, name, image_url FROM products WHERE image_url IS NOT NULL LIMIT 10").fetchall()
    for r in rows:
        print(f"ID: {r[0]}")
        print(f"Name: {r[1]}")
        print(f"URL: {r[2]}")
        print("-" * 20)

    conn.close()

if __name__ == "__main__":
    check_images()
