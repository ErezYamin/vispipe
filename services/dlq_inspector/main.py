import json
import os
from datetime import datetime, timezone

import pika

RABBITMQ_URL = os.environ["RABBITMQ_URL"]


def run() -> None:
    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    ch.queue_declare(queue="dlq", durable=True)
    ch.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        timestamp = datetime.now(timezone.utc).isoformat()
        try:
            msg = json.loads(body)
            image_id = msg.get("image_id", "UNKNOWN")
            path = msg.get("path", msg.get("source_path", "UNKNOWN"))
            print(f"[DLQ] {timestamp} | image_id={image_id} | path={path} | raw={body.decode()}")
        except Exception:
            print(f"[DLQ] {timestamp} | UNPARSEABLE message: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue="dlq", on_message_callback=callback)
    print("[dlq_inspector] Running — watching Dead Letter Queue")
    ch.start_consuming()


if __name__ == "__main__":
    run()
