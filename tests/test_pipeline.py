import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../services/preprocessor"))

import numpy as np
import cv2
import pytest
from pipeline import (
    load_image,
    resize,
    to_grayscale,
    normalize,
    preprocess,
    encode_to_png,
    CorruptImageError,
)


class TestLoadImage:
    def test_loads_valid_jpeg(self, valid_image_bytes):
        img = load_image(valid_image_bytes)
        assert img is not None
        assert img.ndim == 3  # H x W x C

    def test_raises_on_corrupt_bytes(self, corrupt_image_bytes):
        with pytest.raises(CorruptImageError):
            load_image(corrupt_image_bytes)

    def test_returns_numpy_array(self, valid_image_bytes):
        img = load_image(valid_image_bytes)
        assert isinstance(img, np.ndarray)


class TestResize:
    def test_resizes_to_224x224(self, valid_image_bytes):
        img = load_image(valid_image_bytes)
        result = resize(img)
        assert result.shape[:2] == (224, 224)

    def test_custom_size(self, valid_image_bytes):
        img = load_image(valid_image_bytes)
        result = resize(img, size=(100, 100))
        assert result.shape[:2] == (100, 100)

    def test_works_on_small_image(self, tiny_image_bytes):
        img = load_image(tiny_image_bytes)
        result = resize(img)
        assert result.shape[:2] == (224, 224)


class TestToGrayscale:
    def test_output_is_2d(self, valid_image_bytes):
        img = load_image(valid_image_bytes)
        gray = to_grayscale(img)
        assert gray.ndim == 2

    def test_output_dtype_is_uint8(self, valid_image_bytes):
        img = load_image(valid_image_bytes)
        gray = to_grayscale(img)
        assert gray.dtype == np.uint8


class TestNormalize:
    def test_values_between_0_and_1(self, valid_image_bytes):
        img = load_image(valid_image_bytes)
        gray = to_grayscale(img)
        norm = normalize(gray)
        assert norm.min() >= 0.0
        assert norm.max() <= 1.0

    def test_output_dtype_is_float32(self, valid_image_bytes):
        img = load_image(valid_image_bytes)
        gray = to_grayscale(img)
        norm = normalize(gray)
        assert norm.dtype == np.float32


class TestPreprocess:
    def test_full_pipeline_produces_224x224_float32(self, valid_image_bytes):
        result = preprocess(valid_image_bytes)
        assert result.shape == (224, 224)
        assert result.dtype == np.float32
        assert result.min() >= 0.0
        assert result.max() <= 1.0

    def test_raises_on_corrupt_input(self, corrupt_image_bytes):
        with pytest.raises(CorruptImageError):
            preprocess(corrupt_image_bytes)


class TestEncodeToPng:
    def test_encode_returns_bytes(self, valid_image_bytes):
        processed = preprocess(valid_image_bytes)
        png_bytes = encode_to_png(processed)
        assert isinstance(png_bytes, bytes)
        assert len(png_bytes) > 0

    def test_encoded_image_is_valid_png(self, valid_image_bytes):
        processed = preprocess(valid_image_bytes)
        png_bytes = encode_to_png(processed)
        arr = np.frombuffer(png_bytes, np.uint8)
        decoded = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
        assert decoded is not None
        assert decoded.shape == (224, 224)
