import os
import googleapiclient.discovery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
SERVICE_ACCOUNT_FILE = os.getenv("YOUTUBE_SERVICE_ACCOUNT_FILE", "account.json")

def get_authenticated_service():
    """Authenticates using the Service Account file defined in .env"""
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"Service account file not found: {SERVICE_ACCOUNT_FILE}")

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return googleapiclient.discovery.build("youtube", "v3", credentials=credentials)

def get_channel_details(youtube, channel_id_or_handle):
    """Fetches channel metadata (Title, Subs, Description, Uploads Playlist ID)."""
    # 1. Determine if input is ID or Handle
    if channel_id_or_handle.startswith("@"):
        request = youtube.channels().list(part="snippet,contentDetails,statistics", forHandle=channel_id_or_handle)
    else:
        request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id_or_handle)

    response = request.execute()

    if not response["items"]:
        return None

    item = response["items"][0]
    return {
        "id": item["id"],
        "title": item["snippet"]["title"],
        "description": item["snippet"]["description"],
        "thumbnail": item["snippet"]["thumbnails"]["high"]["url"],
        "stats": {
            "viewCount": int(item["statistics"].get("viewCount", 0)),
            "subscriberCount": int(item["statistics"].get("subscriberCount", 0)),
            "videoCount": int(item["statistics"].get("videoCount", 0))
        },
        "uploads_playlist": item["contentDetails"]["relatedPlaylists"]["uploads"]
    }

def get_channel_videos(youtube, channel_id, limit=50):
    """
    Fetches the most recent videos from a channel.
    Step 1: Get the 'uploads' playlist ID.
    Step 2: Fetch videos from that playlist.
    """
    # 1. Get the uploads playlist ID
    channel_response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    if not channel_response['items']:
        return []

    uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    videos = []
    next_page_token = None

    # 2. Iterate through the playlist
    while len(videos) < limit:
        # Fetch playlist items
        pl_request = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=min(50, limit - len(videos)),
            pageToken=next_page_token
        )
        pl_response = pl_request.execute()

        for item in pl_response['items']:
            vid_id = item['snippet']['resourceId']['videoId']
            videos.append({
                "id": vid_id,
                "title": item['snippet']['title'],
                "upload_date": item['snippet']['publishedAt']
            })

        next_page_token = pl_response.get('nextPageToken')
        if not next_page_token:
            break

    return videos

def get_video_metadata(youtube, video_id):
    """Fetches metadata for a single video."""
    request = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=video_id
    )
    response = request.execute()

    if not response["items"]:
        return None

    item = response["items"][0]

    # Extract tags safely
    tags = item["snippet"].get("tags", [])

    return {
        "id": item["id"],
        "title": item["snippet"]["title"],
        "description": item["snippet"]["description"],
        "channel_id": item["snippet"]["channelId"],
        "channel_name": item["snippet"]["channelTitle"],
        "upload_date": item["snippet"]["publishedAt"],
        "thumbnail": item["snippet"]["thumbnails"]["high"]["url"],
        "tags": tags,
        "category": item["snippet"].get("categoryId"), # Category ID (e.g., "22" for People & Blogs)
        "stats": {
            "viewCount": int(item["statistics"].get("viewCount", 0)),
            "likeCount": int(item["statistics"].get("likeCount", 0)),
            "commentCount": int(item["statistics"].get("commentCount", 0))
        },
        "duration": item["contentDetails"]["duration"]
    }
