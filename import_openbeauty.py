import csv
import sqlite3
import sys
import os
import argparse
from config import DB_PATH

def normalize(text):
    if not text:
        return ""
    return text.strip().lower()

def import_data(csv_file_path, limit=None):
    if not os.path.exists(csv_file_path):
        print(f"[Error] File not found: {csv_file_path}")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    print("Loading existing brands to cache...")
    # Cache existing brands to minimize DB reads {normalized_name: id}
    existing_brands = {}
    for row in c.execute("SELECT id, normalized_name FROM brands"):
        existing_brands[row[1]] = row[0]
    
    print(f"Loaded {len(existing_brands)} existing brands.")

    print(f"Processing {csv_file_path}...")
    
    # Open Beauty Facts CSV uses tab separation usually
    with open(csv_file_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        
        count = 0
        new_brands = 0
        new_products = 0
        
        # Batch insert buffer
        # We process line by line but commit in batches for speed
        for row in reader:
            if limit and count >= limit:
                break

            # 1. Extract Fields
            product_name = row.get('product_name', '').strip()
            brands_str = row.get('brands', '').strip()
            categories = row.get('categories', '').strip()
            
            if not product_name:
                continue

            # 2. Handle Brand
            # OBF often lists multiple brands like "L'Oreal, L'Oreal Paris". We take the first one.
            brand_name = brands_str.split(',')[0].strip()
            brand_id = None

            if brand_name:
                b_norm = normalize(brand_name)
                if b_norm in existing_brands:
                    brand_id = existing_brands[b_norm]
                else:
                    # Insert new brand
                    try:
                        c.execute(
                            "INSERT INTO brands (name, normalized_name, category) VALUES (?, ?, ?)",
                            (brand_name, b_norm, "Beauty")
                        )
                        brand_id = c.lastrowid
                        existing_brands[b_norm] = brand_id
                        new_brands += 1
                    except sqlite3.IntegrityError:
                        # Race condition or duplicate ignored
                        pass

            # 3. Insert Product
            # We use 'INSERT OR IGNORE' to skip duplicates if the name already exists
            p_norm = normalize(product_name)
            try:
                c.execute("""
                    INSERT OR IGNORE INTO products (name, normalized_name, brand_name, brand_id, meta)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    product_name, 
                    p_norm, 
                    brand_name, 
                    brand_id, 
                    f'{{"categories": "{categories}"}}'
                ))
                if c.rowcount > 0:
                    new_products += 1
            except Exception as e:
                print(f"Error inserting product {product_name}: {e}")

            count += 1
            if count % 1000 == 0:
                conn.commit()
                print(f"Processed {count} rows... (Brands: {new_brands}, Products: {new_products})")

    conn.commit()
    conn.close()
    print("------------------------------------------------")
    print(f"Done! Total Processed: {count}")
    print(f"New Brands Added: {new_brands}")
    print(f"New Products Added: {new_products}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import Open Beauty Facts Data")
    parser.add_argument("file", help="Path to the downloaded CSV file")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows to process (for testing)")
    
    args = parser.parse_args()
    
    import_data(args.file, args.limit)
