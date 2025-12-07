import re

def extract_socials(text):
    if not text:
        return {}

    socials = {}
    
    # 1. Email (Simple Pattern)
    email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    if email_match:
        socials['email'] = email_match.group(0)

    # 2. Instagram (Capture handle)
    # Matches: instagram.com/jessyluxe or @jessyluxe
    ig_match = re.search(r'(?:instagram\.com\/|@)([\w\.]+)', text)
    if ig_match and 'tiktok' not in ig_match.group(0): # Avoid false positives
        socials['instagram'] = ig_match.group(1)

    # 3. TikTok
    tt_match = re.search(r'tiktok\.com\/(@[\w\.]+)', text)
    if tt_match:
        socials['tiktok'] = tt_match.group(1)

    # 4. Twitter/X
    tw_match = re.search(r'(?:twitter\.com|x\.com)\/([\w\.]+)', text)
    if tw_match:
        socials['twitter'] = tw_match.group(1)
        
    # 5. Spotify (Capture User ID or Artist ID)
    sp_match = re.search(r'open\.spotify\.com\/(?:user|artist)\/([\w\d]+)', text)
    if sp_match:
        socials['spotify'] = sp_match.group(1)

    # 6. Soundcloud
    sc_match = re.search(r'soundcloud\.com\/([\w\d-]+)', text)
    if sc_match:
        socials['soundcloud'] = sc_match.group(1)

    # 7. Website (Look for generic links that aren't the above)
    # This is harder, but we can look for "jessyluxe.com" style links
    # For now, we rely on specific labelled links if possible, or general pattern
    web_match = re.search(r'https?:\/\/(?!www\.(?:youtube|instagram|tiktok|twitter|spotify|soundcloud))([\w\.-]+\.[a-z]{2,})', text)
    if web_match:
        socials['website'] = web_match.group(0)

    return socials
