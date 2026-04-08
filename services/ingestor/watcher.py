import uuid
from datetime import datetime, timezone


def build_ingest_message(object_name: str) -> dict:
    """Build a RabbitMQ message payload for a new raw image."""
    return {
        "image_id": str(uuid.uuid4()),
        "path": object_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
