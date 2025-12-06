# ingestion/youtube_client.py

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


def get_channel_uploads_playlist_id(youtube, channel_id: str) -> str | None:
    """
    Given a channel_id, return the uploads playlist ID.
    """
    try:
        resp = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()

        items = resp.get("items", [])
        if not items:
            return None

        return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    except Exception as e:
        print(f"Error fetching uploads playlist for {channel_id}: {e}")
        return None


def get_latest_video_ids(youtube, playlist_id: str, limit: int = 20) -> list[str]:
    """
    List the most recent video IDs from a channel's uploads playlist.
    """
    video_ids = []
    page_token = None

    while len(video_ids) < limit:
        try:
            resp = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=min(50, limit - len(video_ids)),
                pageToken=page_token
            ).execute()

            items = resp.get("items", [])
            if not items:
                break

            for item in resp.get("items", []):
                vid = item["contentDetails"]["videoId"]
                video_ids.append(vid)

            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        except Exception as e:
            print(f"Error fetching playlist items: {e}")
            break

    return video_ids


def get_video_metadata(youtube, video_id: str) -> dict | None:
    """
    Fetch core metadata for a single video.
    """
    try:
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

        video_meta = {
            "id": video_id,
            "channel_id": snippet["channelId"],
            "title": snippet["title"],
            "channel": snippet["channelTitle"],
            "date": snippet["publishedAt"],
            "duration": content_details.get("duration", ""),
            "viewCount": int(stats.get("viewCount", 0) or 0),
            "likeCount": int(stats.get("likeCount", 0) or 0),
            "thumbnail": snippet["thumbnails"].get("high", {}).get("url", ""),
            "author": snippet["channelTitle"],
            "isFamilySafe": True,
            "ownerProfileUrl": "",
            "category": "",
        }
        return video_meta
    except Exception as e:
        print(f"Error fetching video metadata for {video_id}: {e}")
        return None


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
            "thumbnail_url": snippet["thumbnails"].get("default", {}).get("url"),
            "creation_date": snippet.get("publishedAt"),
            "platform": "YouTube"
        }
    except Exception as e:
        print(f"Error fetching channel details for {channel_id}: {e}")
        return None
