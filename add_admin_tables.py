import sqlite3
from config import DB_PATH

def add_admin_tables():
    print(f"--- Updating Schema for Admin Logs ---")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 1. Ingestion Logs Table
    c.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT,
            channel_id TEXT,
            status TEXT,        -- 'SUCCESS', 'FAILED', 'SKIPPED'
            step TEXT,          -- 'METADATA', 'TRANSCRIPT', 'DB_SAVE'
            error_message TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 2. Indexes for fast dashboard loading
    c.execute("CREATE INDEX IF NOT EXISTS idx_logs_status ON ingestion_logs(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_logs_date ON ingestion_logs(timestamp)")
    
    conn.commit()
    conn.close()
    print("✅ Admin tables created.")

if __name__ == "__main__":
    add_admin_tables()
