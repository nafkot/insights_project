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

    if os.path.exists(cache_file):
        print(f"[{video_id}] Using cached RapidAPI transcript")
        with open(cache_file, "r") as f:
            return json.load(f)

    print(f"[{video_id}] Fetching transcript from RapidAPI...")

    # Build URL & headers
    url = f"https://{RAPIDAPI_HOST}/download-all/{video_id}"
    params = {"format_subtitle": "json", "format_answer": "json"}
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }

    # === DEBUG LOG BLOCK ===
    print("\n================ RAPIDAPI DEBUG ================")
    print("VIDEO ID:", video_id)
    print("RapidAPI Host:", RAPIDAPI_HOST)
    print("RapidAPI Key (first 8 chars):", RAPIDAPI_KEY[:8], "(full length:", len(RAPIDAPI_KEY), ")")
    print("Request URL:", url)
    print("Request Params:", params)
    print("Request Headers:", headers)
    print("================================================\n")
    # ==================================================

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)

        print(f"[{video_id}] RapidAPI Response Status:", resp.status_code)

        # Debug the raw server response
        try:
            print(f"[{video_id}] RapidAPI Raw Response Snippet:", resp.text[:500])
        except:
            print(f"[{video_id}] (Unable to print response text)")

        # Expect 200 for success
        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as json_err:
                print(f"[{video_id}] JSON parsing failed:", json_err)
                return None

            # RapidAPI provider returns a LIST of tracks
            if not isinstance(data, list):
                print(f"[{video_id}] Unexpected JSON shape (expected list):", type(data))
                print("Full data:", data)
                return None

            segments = []

            # Correct parsing for provider returning: [{"subtitle":[...]}]
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
                with open(cache_file, "w") as f:
                    json.dump(segments, f, indent=2)
                print(f"[{video_id}] RapidAPI transcript extracted ({len(segments)} segments)")
                return segments

            print(f"[{video_id}] RapidAPI returned 200 but no segments could be extracted.")
            return None        

        else:
            print(f"[{video_id}] RapidAPI returned status {resp.status_code}")
            return None

    except Exception as e:
        print(f"[{video_id}] RapidAPI transcript error:", e)
        return None


def download_audio_with_ytdlp(video_id: str, use_proxy: bool, use_cookies: bool) -> str | None:
    """
    Download audio using yt-dlp with optional proxy & cookies.
    Returns the path to the downloaded audio file, or None if it fails.
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
        print(f"[{video_id}] yt-dlp download error (proxy={use_proxy}, cookies={use_cookies}): {e}")
        return None

    # Find best match file
    for ext in ("webm", "m4a", "mp3", "wav"):
        candidate = os.path.join(AUDIO_OUTPUT_DIR, f"{video_id}.{ext}")
        if os.path.exists(candidate):
            return candidate

    return None


def transcribe_audio_local(audio_path: str) -> List[Dict[str, Any]] | None:
    """
    Transcribe using Whisper (local or external service).
    For now, this function assumes a remote API, but you can replace it
    with your own local whisper call.
    Returns segments [{start, end, text}, ...] or None.
    """
    if WHISPER_API_URL and WHISPER_API_KEY:
        # Example: remote whisper API (you will adapt to your own endpoint format)
        try:
            with open(audio_path, "rb") as f:
                files = {"file": f}
                headers = {"Authorization": f"Bearer {WHISPER_API_KEY}"}
                resp = requests.post(WHISPER_API_URL, headers=headers, files=files, timeout=600)

            if resp.status_code == 200:
                data = resp.json()
                # Expect data["segments"] in standard openai-whisper-like shape.
                segments = []
                for seg in data.get("segments", []):
                    segments.append({
                        "start": float(seg.get("start", 0.0)),
                        "end": float(seg.get("end", 0.0)),
                        "text": seg.get("text", "").strip(),
                    })
                return segments

            print(f"[ASR] Whisper API failed with status {resp.status_code}")
            return None
        except Exception as e:
            print(f"[ASR] Whisper API error: {e}")
            return None
    else:
        # Placeholder: local whisper CLI example
        # Replace this with your own local pipeline if you already have one.
        print(f"[ASR] No WHISPER_API configured; you must implement local transcription for {audio_path}.")
        return None


def build_transcript_segments_from_audio(video_id: str, use_proxy: bool, use_cookies: bool) -> List[Dict[str, Any]] | None:
    """
    Download audio and run transcription, returning segments or None.
    """
    audio_path = download_audio_with_ytdlp(video_id, use_proxy=use_proxy, use_cookies=use_cookies)
    if not audio_path:
        return None

    print(f"[{video_id}] Transcribing audio: {audio_path}")
    segments = transcribe_audio_local(audio_path)
    return segments


def get_transcript_segments(video_id: str) -> List[Dict[str, Any]] | None:
    """
    Top-level pipeline for transcript ingestion with 3 fallbacks:

    1) RapidAPI
    2) yt-dlp + proxy + ASR
    3) yt-dlp + cookies + ASR
    """
    # OPTION 1: RapidAPI
    segments = try_rapidapi_transcript(video_id)
    if segments:
        print(f"[{video_id}] Using RapidAPI transcript ({len(segments)} segments).")
        return segments

    # OPTION 2: yt-dlp with proxy
    print(f"[{video_id}] RapidAPI failed, trying yt-dlp with proxy...")
    segments = build_transcript_segments_from_audio(video_id, use_proxy=True, use_cookies=False)
    if segments:
        print(f"[{video_id}] Using proxy-download transcript ({len(segments)} segments).")
        return segments

    # OPTION 3: yt-dlp with cookies
    print(f"[{video_id}] Proxy download failed, trying yt-dlp with cookies...")
    segments = build_transcript_segments_from_audio(video_id, use_proxy=False, use_cookies=True)
    if segments:
        print(f"[{video_id}] Using cookies-download transcript ({len(segments)} segments).")
        return segments

    print(f"[{video_id}] All transcript methods failed.")
    return None

