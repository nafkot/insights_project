#!/bin/bash

# 1. Safety Check: Ensure transcript cache exists so we don't lose data
if [ -d "transcript_cache" ]; then
    echo "✅ Transcript cache found. Your downloaded transcripts are safe."
else
    echo "⚠️ Warning: No transcript_cache folder found."
fi

# 2. Remove the existing database (This resets the tables, but NOT the files)
if [ -f "youtube_insights.db" ]; then
    echo "♻️  Removing existing database (Wiping tables)..."
    rm youtube_insights.db
else
    echo "ℹ️  No database found to remove."
fi

# 3. Re-initialize the database schema
echo "🛠  Initializing new database schema..."
python3 db_init.py

echo "--------------------------------------------------------"
echo "✅ Server reset complete."
echo "   - Database is empty and ready for new analysis."
echo "   - Transcripts in 'transcript_cache/' will be reused (No API cost)."
echo "--------------------------------------------------------"
