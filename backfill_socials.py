import sqlite3
from config import DB_PATH
from utils.social_extractor import extract_socials

def backfill_socials():
    print(f"--- Backfilling Social Links for channels in {DB_PATH} ---")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 1. Get all channels
    channels = c.execute("SELECT channel_id, title, description FROM channels").fetchall()
    print(f"Scanning {len(channels)} channels...")

    updated_count = 0

    for ch in channels:
        # 2. Extract
        desc = ch["description"] or ""
        socials = extract_socials(desc)

        # Skip if no socials found
        if not socials:
            continue

        # 3. Update Database
        # We construct the SQL dynamically based on what was found to be efficient
        update_fields = []
        params = []

        for key, value in socials.items():
            update_fields.append(f"{key} = ?")
            params.append(value)
        
        if update_fields:
            params.append(ch["channel_id"])
            sql = f"UPDATE channels SET {', '.join(update_fields)} WHERE channel_id = ?"
            c.execute(sql, params)
            updated_count += 1
            print(f"✅ Updated {ch['title']}: {list(socials.keys())}")

    conn.commit()
    conn.close()
    print("-" * 40)
    print(f"Done! Updated {updated_count} channels with new social links.")

if __name__ == "__main__":
    backfill_socials()
