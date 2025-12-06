#!/bin/bash

# 1. Remove the existing database
if [ -f "youtube_insights.db" ]; then
    echo "Removing existing database..."
    rm youtube_insights.db
else
    echo "No database found to remove."
fi

# 2. Re-initialize the database schema
echo "Initializing new database..."
python3 db_init.py

echo "Server reset complete."
