import os
import requests

def upload_to_abyss(file_path=None, api_key=None, progress_callback=None, file_like=None):
    """
    Uploads to Abyss. Supports either a real file path (file_path) or a file-like object (file_like)
    """
    if file_like:
        files = {"file": ("video.mp4", file_like, "video/mp4")}
    elif file_path:
        f = open(file_path, "rb")
        files = {"file": (os.path.basename(file_path), f, "video/mp4")}
    else:
        raise ValueError("Either file_path or file_like must be provided")

    r
