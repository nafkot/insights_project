# ingestion/youtube_client.py

from config import *

import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

from config import YOUTUBE_SERVICE_ACCOUNT_FILE, YOUTUBE_SCOPE


def get_authenticated_service():
    """
    Authenticate using the service account JSON file and return a YouTube API client.
    """
    creds = service_account.Credentials.from_service_account_file(
        YOUTUBE_SERVICE_ACCOUNT_FILE,
        scopes=YOUTUBE_SCOPE
    )
    youtube = build('youtube', 'v3', credentials=creds, cache_discovery=False)
    return youtube


def get_channel_details(youtube, channel_id: str) -> dict | None:
    """
    Fetch detailed channel info: subscriber count, avatar, description.
    """
    try:
        resp = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        ).execute()

        items = resp.get("items", [])
        if not items:
            return None

        item = items[0]
        snippet = item["snippet"]
        stats = item["statistics"]

        return {
            "channel_id": channel_id,
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "video_count": int(stats.get("videoCount", 0)),
            "view_count": int(stats.get("viewCount", 0)),
            "thumbnail_url": snippet["thumbnails"].get("default", {}).get("url"), # avatar
            "creation_date": snippet.get("publishedAt"),
            "platform": "YouTube"
        }
    except Exception as e:
        print(f"Error fetching channel details for {channel_id}: {e}")
        return None

def get_latest_video_ids(youtube, playlist_id: str, limit: int = 20) -> list[str]:
    """
    List the most recent video IDs from a channel's uploads playlist.
    """
    video_ids = []
    page_token = None

    while len(video_ids) < limit:
        resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=min(50, limit - len(video_ids)),
            pageToken=page_token
        ).execute()

        for item in resp.get("items", []):
            vid = item["contentDetails"]["videoId"]
            video_ids.append(vid)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return video_ids


def get_video_metadata(youtube, video_id: str) -> dict | None:
    """
    Fetch core metadata for a single video and normalize into our video_meta format.
    """
    resp = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=video_id
    ).execute()

    items = resp.get("items", [])
    if not items:
        return None

    item = items[0]
    snippet = item["snippet"]
    stats = item.get("statistics", {})
    content_details = item.get("contentDetails", {})

    # You can refine this parsing (duration, etc.) as needed
    video_meta = {
        "id": video_id,
        "channel_id": snippet["channelId"],
        "title": snippet["title"],
        "channel": snippet["channelTitle"],
        "date": snippet["publishedAt"],
        "duration": content_details.get("duration", ""),  # ISO 8601; can parse later
        "viewCount": int(stats.get("viewCount", 0) or 0),
        "likeCount": int(stats.get("likeCount", 0) or 0),
        "thumbnail": snippet["thumbnails"].get("high", {}).get("url", ""),
        "author": snippet["channelTitle"],
        "isFamilySafe": True,  # or use contentRating if needed
        "ownerProfileUrl": "",
        "category": "",        # can map from categoryId if you want later
    }
    return video_meta

