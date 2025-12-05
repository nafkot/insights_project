# Insights Project — YouTube & Social Media Analytics Engine

This project ingests YouTube channels, extracts transcripts, identifies brands, products, sponsors, computes sentiment, and generates dashboards via Flask.

## Features
- RapidAPI → Transcript extraction
- yt-dlp fallback with proxies and cookies
- Deterministic LLM brand/product/sponsor extraction
- Automatic DB ingestion
- Flask front-end:
  - Brand dashboards
  - Product dashboards
  - Sponsor dashboards
  - Channel profiles
  - Smart autocomplete
  - Hybrid semantic search (DB + LLM)

## Tech Stack
- Python 3.12
- Flask
- SQLite (future: Postgres)
- OpenAI API (GPT-4.1-mini + GPT-4.1)
- Chart.js front-end graphs

## Quick Start
```bash
pip install -r requirements.txt
python3 db_init.py
python3 ingest_channel.py --channel <CHANNEL_ID> --max-videos 20
python3 web/app.py
