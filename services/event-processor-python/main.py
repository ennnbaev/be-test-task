"""
event-processor (Python)

Consumes events from a Kafka topic, transforms each JSON payload into XML,
and stores the result in a Minio bucket.
"""

import json
import logging
import os
import sys
from io import BytesIO

import xmltodict
from confluent_kafka import Consumer
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
TOPIC    = os.getenv("KAFKA_TOPIC", "events")
BUCKET   = os.getenv("MINIO_BUCKET", "events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "event-processor-group")


def ensure_bucket(client: Minio) -> None:
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        log.info("Created bucket %r", BUCKET)


def process_message(minio_client: Minio, raw: bytes) -> None:
    data = json.loads(raw.decode("utf-8"))

    # Use the event id as the Minio object key so the object is
    # deterministically addressable and re-processing is idempotent.
    event_id = data.get("id")
    if not event_id:
        log.warning("Message has no 'id' field, skipping: %s", data)
        return

    xml = xmltodict.unparse({"event": data}, pretty=True)
    object_key = f"{event_id}.xml"
    body = xml.encode("utf-8")
    minio_client.put_object(BUCKET, object_key, BytesIO(body), length=len(body))
    log.info("Stored event %s → %s", event_id, object_key)


def main() -> None:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    ensure_bucket(minio_client)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    log.info("Subscribed to topic %r", TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue

            try:
                process_message(minio_client, msg.value())
            except Exception:
                log.exception("Failed to process message at offset %s", msg.offset())
    finally:
        consumer.close()
        log.info("Consumer closed")


if __name__ == "__main__":
    main()
