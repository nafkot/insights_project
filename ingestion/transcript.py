import os
import json
import requests
import yt_dlp
import re
import glob
import random
from config import RAPIDAPI_KEY

TRANSCRIPT_CACHE_DIR = "transcript_cache"
COOKIES_FILE = "cookies.txt"
PROXY_LIST_URL = "https://free.redscrape.com/api/proxies"
WORKING_PROXIES = []

# --- CONFIG ---
# Hardcoded to match your working CURL request
RAPIDAPI_HOST = "youtube-captions-transcript-subtitles-video-combiner.p.rapidapi.com"
RAPIDAPI_BASE_URL = f"https://{RAPIDAPI_HOST}/download-all"

class SilentLogger:
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass

def ensure_cache_dir():
    if not os.path.exists(TRANSCRIPT_CACHE_DIR):
        os.makedirs(TRANSCRIPT_CACHE_DIR)

def fetch_proxies():
    global WORKING_PROXIES
    try:
        print("[Proxy] Fetching new proxy list...")
        response = requests.get(PROXY_LIST_URL, timeout=5)
        if response.status_code == 200:
            WORKING_PROXIES = [p.strip() for p in response.text.strip().split('\n') if p.strip()]
            print(f"[Proxy] Found {len(WORKING_PROXIES)} proxies.")
    except Exception as e:
        print(f"[Proxy] Failed to fetch list: {e}")
        WORKING_PROXIES = []

def get_random_proxy():
    if not WORKING_PROXIES:
        fetch_proxies()
    return random.choice(WORKING_PROXIES) if WORKING_PROXIES else None

def parse_srt_content(srt_text):
    """Parses SRT string into segments."""
    segments = []
    pattern = re.compile(r'(\d{2}:\d{2}:\d{2}[,.]\d{3}) --> (\d{2}:\d{2}:\d{2}[,.]\d{3})')

    def time_to_sec(t_str):
        t_str = t_str.replace(',', '.')
        parts = t_str.split(':')
        sec = float(parts[-1])
        if len(parts) > 1: sec += int(parts[-2]) * 60
        if len(parts) > 2: sec += int(parts[-3]) * 3600
        return sec

    for line in srt_text.split('\n'):
        line = line.strip()
        if not line or line.isdigit(): continue
        match = pattern.search(line)
        if match:
            segments.append({"start": time_to_sec(match.group(1)), "duration": 0, "text": ""})
            segments[-1]["end_temp"] = time_to_sec(match.group(2)) # Store end time temporarily
        elif segments:
            clean = re.sub(r'<[^>]+>', '', line)
            segments[-1]["text"] += clean + " "

    # Clean up duration
    final_segments = []
    for s in segments:
        if s["text"].strip():
            s["duration"] = round(s.pop("end_temp") - s["start"], 2)
            s["text"] = s["text"].strip()
            final_segments.append(s)

    return final_segments

def parse_vtt_file(vtt_path):
    with open(vtt_path, 'r', encoding='utf-8') as f:
        return parse_srt_content(f.read())

def download_with_ytdlp(video_id, use_proxy=False, use_cookies=False):
    temp_out = os.path.join(TRANSCRIPT_CACHE_DIR, f"temp_{video_id}")
    ydl_opts = {
        'skip_download': True,
        'writesubtitles': True,
        'writeautomaticsub': True,
        'subtitleslangs': ['en'],
        'outtmpl': temp_out,
        'socket_timeout': 10,
        'retries': 2,
        'quiet': True,
        'no_warnings': True,
        'logger': SilentLogger(),
    }

    method_name = "Standard"
    if use_proxy:
        proxy = get_random_proxy()
        if proxy:
            ydl_opts['proxy'] = proxy
            method_name = f"Proxy ({proxy.split(':')[0]})"
        else: return None

    if use_cookies:
        if os.path.exists(COOKIES_FILE):
            ydl_opts['cookiefile'] = COOKIES_FILE
            method_name = "Cookies"
        else: return None

    print(f"[{video_id}] Attempting via {method_name}...", end=" ", flush=True)

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
            except Exception as e:
                if "no subtitles" in str(e).lower():
                    print("✗ No Subs.")
                    return None
                raise e

        vtt_files = glob.glob(f"{temp_out}*.vtt")
        if vtt_files:
            print("✓ Success.")
            segments = parse_vtt_file(vtt_files[0])
            os.remove(vtt_files[0])
            return segments
    except:
        print("✗ Failed.")

    return None

def get_transcript_segments(video_id):
    ensure_cache_dir()

    # 0. SMARTER CACHE CHECK (Checks multiple filenames)
    possible_names = [
        f"{video_id}.json",
        f"{video_id}_rapidapi.json",
        f"{video_id}_from_rapidapi.json"
    ]

    for fname in possible_names:
        cache_path = os.path.join(TRANSCRIPT_CACHE_DIR, fname)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Handle raw RapidAPI array vs clean segment array
                    if isinstance(data, list) and len(data) > 0 and "subtitle" in data[0]:
                        print(f"[{video_id}] Found raw cache ({fname}). Parsing...")
                        return parse_srt_content(data[0]["subtitle"])

                    print(f"[{video_id}] Using cached transcript ({fname}).")
                    return data
            except: pass

    segments = None

    # PRIORITY 1: RapidAPI
    if RAPIDAPI_KEY:
        try:
            key_preview = RAPIDAPI_KEY[:5] + "..." if RAPIDAPI_KEY else "None"
            url = f"{RAPIDAPI_BASE_URL}/{video_id}"
            params = {"format_subtitle": "srt", "format_answer": "json"}
            headers = {
                "x-rapidapi-key": RAPIDAPI_KEY,
                "x-rapidapi-host": RAPIDAPI_HOST
            }

            response = requests.get(url, headers=headers, params=params, timeout=15)

            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0 and "subtitle" in data[0]:
                    print(f"[{video_id}] ✓ Success (RapidAPI).")
                    segments = parse_srt_content(data[0]["subtitle"])
            elif response.status_code == 403:
                print(f"[{video_id}] RapidAPI 403 (Check Key: {key_preview}).")
            elif response.status_code == 429:
                print(f"[{video_id}] RapidAPI 429 (Quota Limit).")

        except Exception as e:
            print(f"[{video_id}] RapidAPI Error: {str(e).split(':')[0]}")

    # PRIORITY 2: Proxy
    if not segments:
        segments = download_with_ytdlp(video_id, use_proxy=True, use_cookies=False)

    # PRIORITY 3: Cookie
    if not segments:
        segments = download_with_ytdlp(video_id, use_proxy=False, use_cookies=True)

    # Final Save
    if segments:
        std_path = os.path.join(TRANSCRIPT_CACHE_DIR, f"{video_id}.json")
        with open(std_path, "w", encoding="utf-8") as f:
            json.dump(segments, f)
        return segments

    print(f"[{video_id}] Skipping (No transcript found).")
    return None
