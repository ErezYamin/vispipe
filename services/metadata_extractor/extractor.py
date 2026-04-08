import hashlib
import cv2
import numpy as np


def compute_checksum(data: bytes) -> str:
    """Return MD5 hex digest of raw bytes."""
    return hashlib.md5(data).hexdigest()


def extract_metadata(
    image_bytes: bytes,
    image_id: str,
    source_path: str,
    processed_path: str,
) -> dict:
    """
    Extract metadata from a preprocessed PNG image.
    Expects a grayscale image (single channel).
    """
    arr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
    if img is None:
        raise ValueError(f"Could not decode image for metadata extraction: {processed_path}")
    height, width = img.shape[:2]
    channels = 1 if img.ndim == 2 else img.shape[2]
    return {
        "image_id": image_id,
        "source_path": source_path,
        "processed_path": processed_path,
        "checksum": compute_checksum(image_bytes),
        "width": width,
        "height": height,
        "channels": channels,
        "file_size_bytes": len(image_bytes),
    }
