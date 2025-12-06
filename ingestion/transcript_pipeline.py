# ingestion/transcript_pipeline.py

from config import *
import os
import json
import subprocess
from typing import List, Dict, Any

import requests
import yt_dlp

from config import (
    RAPIDAPI_KEY,
    RAPIDAPI_HOST,
    PROXY_URL,
    COOKIES_FILE,
    AUDIO_OUTPUT_DIR,
    TRANSCRIPT_CACHE_DIR,
    WHISPER_API_URL,
    WHISPER_API_KEY,
)


def _ensure_dirs():
    os.makedirs(AUDIO_OUTPUT_DIR, exist_ok=True)
    os.makedirs(TRANSCRIPT_CACHE_DIR, exist_ok=True)


def try_rapidapi_transcript(video_id: str) -> List[Dict[str, Any]] | None:
    """
    Try to get a JSON transcript from RapidAPI.
    Returns a list of segments: [{start, end, text}, ...] or None.
    """
    if not RAPIDAPI_KEY:
        print(f"[{video_id}] No RapidAPI key found.")
        return None

    _ensure_dirs()
    cache_file = os.path.join(TRANSCRIPT_CACHE_DIR, f"{video_id}_rapidapi.json")

    # --- FIX: Handle Corrupt Cache ---
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                data = json.load(f)
            print(f"[{video_id}] Using cached RapidAPI transcript")
            return data
        except json.JSONDecodeError:
            print(f"[{video_id}] Corrupt cache file found. Deleting and re-fetching...")
            try:
                os.remove(cache_file)
            except OSError:
                pass

    print(f"[{video_id}] Fetching transcript from RapidAPI...")

    # Build URL & headers
    url = f"https://{RAPIDAPI_HOST}/download-all/{video_id}"
    params = {"format_subtitle": "json", "format_answer": "json"}
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)

        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as json_err:
                print(f"[{video_id}] RapidAPI response JSON parsing failed: {json_err}")
                return None

            if not isinstance(data, list):
                # Sometimes API returns error object
                return None

            segments = []

            for track in data:
                subtitle_list = track.get("subtitle", [])
                for item in subtitle_list:
                    try:
                        start = float(item.get("start", 0.0))
                    except:
                        start = 0.0
                    try:
                        dur = float(item.get("dur", 0.0))
                    except:
                        dur = 0.0

                    text = item.get("text", "").strip()
                    if text:
                        segments.append({
                            "start": start,
                            "end": start + dur,
                            "text": text
                        })

            if segments:
                # Atomically write (or just write)
                with open(cache_file, "w") as f:
                    json.dump(segments, f, indent=2)
                print(f"[{video_id}] RapidAPI transcript extracted ({len(segments)} segments)")
                return segments

            return None

        else:
            print(f"[{video_id}] RapidAPI returned status {resp.status_code}")
            return None

    except Exception as e:
        print(f"[{video_id}] RapidAPI transcript error: {e}")
        return None


def download_audio_with_ytdlp(video_id: str, use_proxy: bool, use_cookies: bool) -> str | None:
    """
    Download audio using yt-dlp with optional proxy & cookies.
    """
    _ensure_dirs()
    output_tmpl = os.path.join(AUDIO_OUTPUT_DIR, f"{video_id}.%(ext)s")

    ytdlp_opts = {
        "format": "bestaudio/best",
        "outtmpl": output_tmpl,
        "quiet": True,
        "no_warnings": True,
    }

    if use_proxy and PROXY_URL:
        ytdlp_opts["proxy"] = PROXY_URL

    if use_cookies and os.path.exists(COOKIES_FILE):
        ytdlp_opts["cookiefile"] = COOKIES_FILE

    url = f"https://www.youtube.com/watch?v={video_id}"

    try:
        with yt_dlp.YoutubeDL(ytdlp_opts) as ydl:
            ydl.download([url])
    except Exception as e:
        print(f"[{video_id}] yt-dlp download error: {e}")
        return None

    for ext in ("webm", "m4a", "mp3", "wav"):
        candidate = os.path.join(AUDIO_OUTPUT_DIR, f"{video_id}.{ext}")
        if os.path.exists(candidate):
            return candidate

    return None


def transcribe_audio_local(audio_path: str) -> List[Dict[str, Any]] | None:
    """
    Transcribe using Whisper (local or external service).
    """
    if WHISPER_API_URL and WHISPER_API_KEY:
        try:
            with open(audio_path, "rb") as f:
                files = {"file": f}
                headers = {"Authorization": f"Bearer {WHISPER_API_KEY}"}
                resp = requests.post(WHISPER_API_URL, headers=headers, files=files, timeout=600)

            if resp.status_code == 200:
                data = resp.json()
                segments = []
                for seg in data.get("segments", []):
                    segments.append({
                        "start": float(seg.get("start", 0.0)),
                        "end": float(seg.get("end", 0.0)),
                        "text": seg.get("text", "").strip(),
                    })
                return segments
            return None
        except Exception as e:
            print(f"[ASR] Whisper API error: {e}")
            return None
    else:
        print(f"[ASR] No WHISPER_API configured.")
        return None


def build_transcript_segments_from_audio(video_id: str, use_proxy: bool, use_cookies: bool) -> List[Dict[str, Any]] | None:
    audio_path = download_audio_with_ytdlp(video_id, use_proxy=use_proxy, use_cookies=use_cookies)
    if not audio_path:
        return None

    print(f"[{video_id}] Transcribing audio: {audio_path}")
    segments = transcribe_audio_local(audio_path)
    return segments


def get_transcript_segments(video_id: str) -> List[Dict[str, Any]] | None:
    # 1. RapidAPI
    segments = try_rapidapi_transcript(video_id)
    if segments:
        return segments

    # 2. yt-dlp + proxy
    print(f"[{video_id}] RapidAPI failed, trying yt-dlp with proxy...")
    segments = build_transcript_segments_from_audio(video_id, use_proxy=True, use_cookies=False)
    if segments:
        return segments

    # 3. yt-dlp + cookies
    print(f"[{video_id}] Proxy download failed, trying yt-dlp with cookies...")
    segments = build_transcript_segments_from_audio(video_id, use_proxy=False, use_cookies=True)
    if segments:
        return segments

    print(f"[{video_id}] All transcript methods failed.")
    return None
