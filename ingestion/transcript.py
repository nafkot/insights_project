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

def ensure_cache_dir():
    if not os.path.exists(TRANSCRIPT_CACHE_DIR):
        os.makedirs(TRANSCRIPT_CACHE_DIR)

def fetch_proxies():
    """Fetches public proxies."""
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

def parse_vtt_file(vtt_path):
    """Parses WebVTT format into clean segments."""
    segments = []
    with open(vtt_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    current_segment = None
    time_pattern = re.compile(r'(\d{2}:\d{2}:\d{2}\.\d{3}) --> (\d{2}:\d{2}:\d{2}\.\d{3})')

    def vtt_timestamp_to_seconds(ts):
        try:
            parts = ts.split(':')
            sec = float(parts[-1])
            if len(parts) > 1: sec += int(parts[-2]) * 60
            if len(parts) > 2: sec += int(parts[-3]) * 3600
            return sec
        except: return 0.0

    for line in lines:
        line = line.strip()
        if not line or 'WEBVTT' in line or 'Kind:' in line or 'Language:' in line: continue

        time_match = time_pattern.search(line)
        if time_match:
            start = vtt_timestamp_to_seconds(time_match.group(1))
            end = vtt_timestamp_to_seconds(time_match.group(2))
            current_segment = {"start": start, "duration": round(end - start, 2), "text": ""}
            segments.append(current_segment)
        elif current_segment:
            # Remove HTML tags like <c.color...>
            clean_text = re.sub(r'<[^>]+>', '', line)
            current_segment["text"] += clean_text + " "

    for s in segments: s["text"] = s["text"].strip()
    return [s for s in segments if s["text"]]

def download_with_ytdlp(video_id, use_proxy=False, use_cookies=False):
    """Downloads subs using yt-dlp with specific network settings."""
    temp_out = os.path.join(TRANSCRIPT_CACHE_DIR, f"temp_{video_id}")

    ydl_opts = {
        'skip_download': True,
        'writesubtitles': True,
        'writeautomaticsub': True,
        'subtitleslangs': ['en'],
        'quiet': True,
        'no_warnings': True,
        'outtmpl': temp_out, # yt-dlp appends .en.vtt
    }

    method_name = "Standard"

    # Configure Proxy
    if use_proxy:
        proxy = get_random_proxy()
        if proxy:
            ydl_opts['proxy'] = proxy
            method_name = f"Proxy ({proxy})"
        else:
            return None # Fail if proxy requested but none available

    # Configure Cookies
    if use_cookies:
        if os.path.exists(COOKIES_FILE):
            ydl_opts['cookiefile'] = COOKIES_FILE
            method_name = "Cookies"
        else:
            print(f"[{video_id}] Cookie file not found.")
            return None

    print(f"[{video_id}] Attempting yt-dlp via {method_name}...")

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

        # Locate the output VTT
        # It might be .en.vtt or .vtt depending on yt-dlp version
        vtt_files = glob.glob(f"{temp_out}*.vtt")

        if vtt_files:
            vtt_path = vtt_files[0]
            segments = parse_vtt_file(vtt_path)
            # Cleanup temp file
            os.remove(vtt_path)
            return segments
    except Exception as e:
        print(f"[{video_id}] {method_name} failed: {e}")

    return None

def get_transcript_segments(video_id):
    ensure_cache_dir()
    cache_path = os.path.join(TRANSCRIPT_CACHE_DIR, f"{video_id}.json")

    # 0. Check Cache First
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass

    segments = None

    # PRIORITY 1: RapidAPI
    if RAPIDAPI_KEY:
        try:
            url = "https://youtube-transcripts.p.rapidapi.com/youtube/transcript"
            headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": "youtube-transcripts.p.rapidapi.com"}
            response = requests.get(url, headers=headers, params={"url": f"https://www.youtube.com/watch?v={video_id}"}, timeout=10)

            if response.status_code == 200 and "content" in response.json():
                print(f"[{video_id}] Success (RapidAPI).")
                segments = response.json()["content"]
            else:
                print(f"[{video_id}] RapidAPI Failed ({response.status_code}).")
        except Exception:
            print(f"[{video_id}] RapidAPI Error.")

    # PRIORITY 2: Proxy (yt-dlp)
    if not segments:
        segments = download_with_ytdlp(video_id, use_proxy=True, use_cookies=False)

    # PRIORITY 3: Cookie (yt-dlp)
    if not segments:
        segments = download_with_ytdlp(video_id, use_proxy=False, use_cookies=True)

    # Final Save
    if segments:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(segments, f)
        return segments

    print(f"[{video_id}] All transcript methods failed.")
    return None
