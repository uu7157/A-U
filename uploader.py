import os
import requests

def upload_to_hydrax(file_path: str, api_key: str, progress_callback=None):
    url = f"http://up.hydrax.net/{api_key}"

    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f, "video/mp4")}
        response = requests.post(url, files=files)

    print("Response text:", response.text)
    response.raise_for_status()

    try:
        data = response.json()
        return data.get("url") or data.get("slug")
    except Exception:
        return None
