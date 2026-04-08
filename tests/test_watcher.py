import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../services/ingestor"))

import pytest
from watcher import build_ingest_message


class TestBuildIngestMessage:
    def test_returns_dict_with_required_keys(self):
        msg = build_ingest_message("bronze/capture_001.jpg")
        assert set(msg.keys()) == {"image_id", "path", "timestamp"}

    def test_path_is_preserved(self):
        msg = build_ingest_message("bronze/test.jpg")
        assert msg["path"] == "bronze/test.jpg"

    def test_image_id_is_valid_uuid(self):
        import uuid
        msg = build_ingest_message("bronze/test.jpg")
        # Should not raise
        uuid.UUID(msg["image_id"])

    def test_each_call_generates_unique_id(self):
        msg1 = build_ingest_message("bronze/test.jpg")
        msg2 = build_ingest_message("bronze/test.jpg")
        assert msg1["image_id"] != msg2["image_id"]

    def test_timestamp_is_iso8601(self):
        from datetime import datetime
        msg = build_ingest_message("bronze/test.jpg")
        # Should not raise
        datetime.fromisoformat(msg["timestamp"])
