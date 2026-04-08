import json
import os

import pika
import psycopg2
from minio import Minio

from extractor import extract_metadata

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def upsert_image(db, meta: dict) -> None:
    """Insert or update image record in the Gold layer."""
    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO images (
                image_id, source_path, processed_path, status,
                checksum, width, height, channels, file_size_bytes, processed_at
            ) VALUES (%s, %s, %s, 'processed', %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (image_id) DO UPDATE SET
                processed_path  = EXCLUDED.processed_path,
                status          = 'processed',
                checksum        = EXCLUDED.checksum,
                width           = EXCLUDED.width,
                height          = EXCLUDED.height,
                channels        = EXCLUDED.channels,
                file_size_bytes = EXCLUDED.file_size_bytes,
                processed_at    = NOW()
            """,
            (
                meta["image_id"],
                meta["source_path"],
                meta["processed_path"],
                meta["checksum"],
                meta["width"],
                meta["height"],
                meta["channels"],
                meta["file_size_bytes"],
            ),
        )
    db.commit()


def run() -> None:
    minio = get_minio_client()
    db = psycopg2.connect(DATABASE_URL)

    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    ch.queue_declare(queue="processed_images", durable=True)
    ch.queue_declare(queue="dlq", durable=True)
    ch.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            bucket, object_name = msg["processed_path"].split("/", 1)
            response = minio.get_object(bucket, object_name)
            try:
                image_bytes = response.read()
            finally:
                response.close()
                response.release_conn()

            meta = extract_metadata(
                image_bytes,
                image_id=msg["image_id"],
                source_path=msg["source_path"],
                processed_path=msg["processed_path"],
            )
            upsert_image(db, meta)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[metadata_extractor] Stored: {meta['image_id']} — {meta['width']}x{meta['height']} px")
        except Exception as e:
            db.rollback()
            print(f"[metadata_extractor] ERROR: {e} — routing to DLQ")
            ch.basic_publish(
                exchange="",
                routing_key="dlq",
                body=body,
                properties=pika.BasicProperties(delivery_mode=2),
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    ch.basic_consume(queue="processed_images", on_message_callback=callback)
    print("[metadata_extractor] Running — consuming from processed_images queue")
    ch.start_consuming()


if __name__ == "__main__":
    run()
