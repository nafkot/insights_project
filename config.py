# config.py
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# SQLite DB
DB_PATH = os.getenv("YOUTUBE_DB", "youtube_insights.db")

# OpenAI / Gemini
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

# YouTube Service Account
YOUTUBE_SERVICE_ACCOUNT_FILE = os.getenv("YOUTUBE_SERVICE_ACCOUNT_FILE", "account.json")
YOUTUBE_SCOPE = ['https://www.googleapis.com/auth/youtube.readonly']

# RapidAPI for transcripts
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST= os.getenv("RAPIDAPI_HOST")

# Transcript / download settings
COOKIES_FILE = os.getenv("YTDLP_COOKIES_FILE", "cookies.txt")
PROXY_URL = os.getenv("YTDLP_PROXY_URL")  # e.g. http://user:pass@host:port
AUDIO_OUTPUT_DIR = os.getenv("AUDIO_OUTPUT_DIR", "audio_cache")
TRANSCRIPT_CACHE_DIR = os.getenv("TRANSCRIPT_CACHE_DIR", "transcript_cache")

# Whisper or external ASR endpoint; adjust as needed
WHISPER_API_URL = os.getenv("WHISPER_API_URL")  # if you call remote whisper
WHISPER_API_KEY = os.getenv("WHISPER_API_KEY")  # optional

