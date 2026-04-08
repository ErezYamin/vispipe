import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../services/metadata_extractor"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../services/preprocessor"))

import pytest
from extractor import compute_checksum, extract_metadata
from pipeline import preprocess, encode_to_png


class TestComputeChecksum:
    def test_returns_32_char_hex_string(self, valid_image_bytes):
        result = compute_checksum(valid_image_bytes)
        assert isinstance(result, str)
        assert len(result) == 32

    def test_same_bytes_same_checksum(self, valid_image_bytes):
        assert compute_checksum(valid_image_bytes) == compute_checksum(valid_image_bytes)

    def test_different_bytes_different_checksum(self, valid_image_bytes, corrupt_image_bytes):
        assert compute_checksum(valid_image_bytes) != compute_checksum(corrupt_image_bytes)


class TestExtractMetadata:
    @pytest.fixture
    def processed_png_bytes(self, valid_image_bytes):
        return encode_to_png(preprocess(valid_image_bytes))

    def test_returns_required_keys(self, processed_png_bytes):
        meta = extract_metadata(
            processed_png_bytes,
            image_id="test-id-123",
            source_path="bronze/img.jpg",
            processed_path="silver/img.png",
        )
        required_keys = {"image_id", "source_path", "processed_path", "checksum",
                         "width", "height", "channels", "file_size_bytes"}
        assert required_keys.issubset(meta.keys())

    def test_dimensions_are_224x224(self, processed_png_bytes):
        meta = extract_metadata(
            processed_png_bytes,
            image_id="test-id-123",
            source_path="bronze/img.jpg",
            processed_path="silver/img.png",
        )
        assert meta["width"] == 224
        assert meta["height"] == 224

    def test_channels_is_1_for_grayscale(self, processed_png_bytes):
        meta = extract_metadata(
            processed_png_bytes,
            image_id="test-id-123",
            source_path="bronze/img.jpg",
            processed_path="silver/img.png",
        )
        assert meta["channels"] == 1

    def test_file_size_matches_bytes_length(self, processed_png_bytes):
        meta = extract_metadata(
            processed_png_bytes,
            image_id="test-id-123",
            source_path="bronze/img.jpg",
            processed_path="silver/img.png",
        )
        assert meta["file_size_bytes"] == len(processed_png_bytes)

    def test_paths_preserved(self, processed_png_bytes):
        meta = extract_metadata(
            processed_png_bytes,
            image_id="abc-123",
            source_path="bronze/capture.jpg",
            processed_path="silver/capture.png",
        )
        assert meta["image_id"] == "abc-123"
        assert meta["source_path"] == "bronze/capture.jpg"
        assert meta["processed_path"] == "silver/capture.png"
