import os
import requests

def upload_to_abyss(file_path: str, api_key: str, progress_callback=None):
    url = f"http://up.hydrax.net/{api_key}"

    with open(file_path, "rb") as f:
        # stream upload
        files = {"file": (os.path.basename(file_path), f)}

        response = requests.post(url, files=files)
        response.raise_for_status()

    data = response.json()
    return data.get("slug")
