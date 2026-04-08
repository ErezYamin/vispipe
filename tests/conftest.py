import pytest
import cv2
import numpy as np


def _make_rgb_image(width=400, height=300) -> np.ndarray:
    """Create a synthetic RGB image with gradients for testing."""
    img = np.zeros((height, width, 3), dtype=np.uint8)
    img[:, :, 0] = np.linspace(0, 255, width, dtype=np.uint8)   # B channel gradient
    img[:, :, 1] = np.linspace(0, 200, width, dtype=np.uint8)   # G channel gradient
    img[:, :, 2] = 128                                           # R channel constant
    return img


def _encode_image(img: np.ndarray, ext: str = ".jpg") -> bytes:
    success, buffer = cv2.imencode(ext, img)
    assert success, "Failed to encode test image"
    return buffer.tobytes()


@pytest.fixture
def valid_image_bytes() -> bytes:
    """JPEG bytes of a valid 400x300 RGB image."""
    return _encode_image(_make_rgb_image())


@pytest.fixture
def valid_png_bytes() -> bytes:
    """PNG bytes of a valid 400x300 RGB image."""
    return _encode_image(_make_rgb_image(), ext=".png")


@pytest.fixture
def corrupt_image_bytes() -> bytes:
    """Bytes that are not a valid image."""
    return b"this is not an image at all"


@pytest.fixture
def tiny_image_bytes() -> bytes:
    """JPEG bytes of a 10x10 image (well under 224x224)."""
    return _encode_image(_make_rgb_image(width=10, height=10))
