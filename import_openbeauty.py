import csv
import sqlite3
import sys
import os
import argparse
import re
from config import DB_PATH

# Regex to detect Non-Latin characters (Chinese, Cyrillic, Arabic, etc.)
# If a name has these, we skip it.
NON_LATIN_PATTERN = re.compile(r'[\u0400-\u04FF\u4e00-\u9fff\u0600-\u06FF\u3040-\u309F\u30A0-\u30FF]')

def normalize(text):
    if not text: return ""
    return text.strip().lower()

def is_safe_name(name):
    """Returns False if name looks like Russian, Chinese, etc."""
    if not name: return False
    if len(name) < 2: return False # Skip 1-letter names
    if NON_LATIN_PATTERN.search(name):
        return False
    return True

def import_data(csv_file_path, limit=None):
    if not os.path.exists(csv_file_path):
        print(f"[Error] File not found: {csv_file_path}")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    print("Loading existing brands cache...")
    existing_brands = {}
    for row in c.execute("SELECT id, normalized_name FROM brands"):
        existing_brands[row[1]] = row[0]

    print(f"Processing {csv_file_path}...")

    # Open with 'replace' to handle bad encoding bytes gracefully
    with open(csv_file_path, 'r', encoding='utf-8', errors='replace') as f:
        # Use tab delimiter as per your sample
        reader = csv.DictReader(f, delimiter='\t')

        count = 0
        skipped = 0
        new_products = 0

        for row in reader:
            if limit and count >= limit: break

            raw_name = row.get('product_name', '').strip()

            # --- FILTER: Skip Non-English / Garbage Names ---
            if not is_safe_name(raw_name):
                skipped += 1
                continue

            brands_str = row.get('brands', '').strip()
            image_url = row.get('image_url', '').strip() or row.get('image_small_url', '').strip()
            categories = row.get('categories', '').strip()
            labels = row.get('labels', '').strip()

            # Handle Brand (Take first one if comma separated)
            brand_name = brands_str.split(',')[0].strip()
            brand_id = None

            if brand_name:
                b_norm = normalize(brand_name)
                if b_norm in existing_brands:
                    brand_id = existing_brands[b_norm]
                else:
                    try:
                        c.execute("INSERT INTO brands (name, normalized_name, category) VALUES (?, ?, ?)",
                                  (brand_name, b_norm, "Beauty"))
                        brand_id = c.lastrowid
                        existing_brands[b_norm] = brand_id
                    except sqlite3.IntegrityError:
                        pass

            # Insert Product with extra fields
            p_norm = normalize(raw_name)
            try:
                # Upsert-like logic: Ignore if exists
                c.execute("""
                    INSERT OR IGNORE INTO products
                    (name, normalized_name, brand_name, brand_id, image_url, main_category, labels)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (raw_name, p_norm, brand_name, brand_id, image_url, categories, labels))

                if c.rowcount > 0:
                    new_products += 1
            except Exception as e:
                pass

            count += 1
            if count % 5000 == 0:
                conn.commit()
                print(f"Scanned {count} rows... (Imported: {new_products}, Skipped Non-Latin: {skipped})")

    conn.commit()
    conn.close()
    print(f"Done! Total Scanned: {count}")
    print(f"Skipped (Foreign Language): {skipped}")
    print(f"New Products Imported: {new_products}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    import_data(args.file, args.limit)
