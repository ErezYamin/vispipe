import json
import os
import time

import pika
from minio import Minio
from minio.error import S3Error

from watcher import build_ingest_message

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]
BRONZE_BUCKET = "bronze"
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def get_rabbitmq_channel():
    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    ch.queue_declare(queue="raw_images", durable=True)
    return conn, ch


def ensure_bucket(minio: Minio, bucket: str) -> None:
    if not minio.bucket_exists(bucket):
        minio.make_bucket(bucket)
        print(f"Created bucket: {bucket}")


def run() -> None:
    minio = get_minio_client()
    ensure_bucket(minio, BRONZE_BUCKET)

    conn, ch = get_rabbitmq_channel()
    seen: set = set()

    print(f"Ingestor running — polling '{BRONZE_BUCKET}' every {POLL_INTERVAL}s")

    while True:
        try:
            for obj in minio.list_objects(BRONZE_BUCKET, recursive=True):
                name = obj.object_name
                if name not in seen:
                    seen.add(name)
                    msg = build_ingest_message(f"{BRONZE_BUCKET}/{name}")
                    ch.basic_publish(
                        exchange="",
                        routing_key="raw_images",
                        body=json.dumps(msg),
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                    print(f"[ingestor] Published: {msg['image_id']} — {name}")
        except S3Error as e:
            print(f"[ingestor] MinIO error: {e}")
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[ingestor] RabbitMQ connection lost: {e} — reconnecting")
            conn, ch = get_rabbitmq_channel()
        except Exception as e:
            print(f"[ingestor] Unexpected error: {e}")

        try:
            conn.sleep(POLL_INTERVAL)   # pika-aware sleep — keeps heartbeat alive
        except pika.exceptions.AMQPConnectionError:
            print("[ingestor] Heartbeat lost during sleep — reconnecting")
            conn, ch = get_rabbitmq_channel()


if __name__ == "__main__":
    run()
