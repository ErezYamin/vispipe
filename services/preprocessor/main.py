import io
import json
import os
from datetime import datetime, timezone

import pika
from minio import Minio

from pipeline import CorruptImageError, encode_to_png, preprocess

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]
SILVER_BUCKET = "silver"


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def ensure_bucket(minio: Minio, bucket: str) -> None:
    if not minio.bucket_exists(bucket):
        minio.make_bucket(bucket)
        print(f"Created bucket: {bucket}")


def process_message(minio: Minio, ch, msg: dict) -> None:
    image_id = msg["image_id"]
    full_path = msg["path"]                      # e.g. "bronze/capture.jpg"
    bucket, object_name = full_path.split("/", 1)

    # Download raw image from Bronze
    response = minio.get_object(bucket, object_name)
    try:
        raw_bytes = response.read()
    finally:
        response.close()
        response.release_conn()

    # Apply preprocessing pipeline
    processed = preprocess(raw_bytes)
    png_bytes = encode_to_png(processed)

    # Use image_id as filename to avoid collisions across source subdirectories
    silver_name = f"{image_id}.png"
    minio.put_object(
        SILVER_BUCKET,
        silver_name,
        io.BytesIO(png_bytes),
        length=len(png_bytes),
        content_type="image/png",
    )

    # Publish downstream message
    out_msg = {
        "image_id": image_id,
        "source_path": full_path,
        "processed_path": f"{SILVER_BUCKET}/{silver_name}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    ch.basic_publish(
        exchange="",
        routing_key="processed_images",
        body=json.dumps(out_msg),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"[preprocessor] Processed: {image_id} → {SILVER_BUCKET}/{silver_name}")


def run() -> None:
    minio = get_minio_client()
    ensure_bucket(minio, SILVER_BUCKET)

    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    ch.queue_declare(queue="raw_images", durable=True)
    ch.queue_declare(queue="processed_images", durable=True)
    ch.queue_declare(queue="dlq", durable=True)
    ch.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            process_message(minio, ch, msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except CorruptImageError as e:
            print(f"[preprocessor] CORRUPT: {e} — routing to DLQ")
            ch.basic_publish(
                exchange="",
                routing_key="dlq",
                body=body,
                properties=pika.BasicProperties(delivery_mode=2),
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"[preprocessor] ERROR: {e} — routing to DLQ")
            ch.basic_publish(
                exchange="",
                routing_key="dlq",
                body=body,
                properties=pika.BasicProperties(delivery_mode=2),
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    ch.basic_consume(queue="raw_images", on_message_callback=callback)
    print("[preprocessor] Running — consuming from raw_images queue")
    ch.start_consuming()


if __name__ == "__main__":
    run()
