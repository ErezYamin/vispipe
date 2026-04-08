import cv2
import numpy as np


class CorruptImageError(Exception):
    pass


def load_image(image_bytes: bytes) -> np.ndarray:
    arr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        raise CorruptImageError("Image could not be decoded — file may be corrupt or unsupported format")
    return img


def resize(img: np.ndarray, size: tuple = (224, 224)) -> np.ndarray:
    return cv2.resize(img, size, interpolation=cv2.INTER_AREA)


def to_grayscale(img: np.ndarray) -> np.ndarray:
    return cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)


def normalize(img: np.ndarray) -> np.ndarray:
    return img.astype(np.float32) / 255.0


def preprocess(image_bytes: bytes) -> np.ndarray:
    """Full pipeline: load → resize (224x224) → grayscale → normalize (0–1)."""
    img = load_image(image_bytes)
    img = resize(img)
    img = to_grayscale(img)
    img = normalize(img)
    return img


def encode_to_png(img: np.ndarray) -> bytes:
    """Convert normalized float32 image back to PNG bytes for storage."""
    img_uint8 = (img * 255).astype(np.uint8)
    success, buffer = cv2.imencode(".png", img_uint8)
    if not success:
        raise RuntimeError("Failed to encode image to PNG")
    return buffer.tobytes()
