"""Kafka producer: polls WeatherAPI and publishes one message per metric."""
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("producer")

running = True


def _stop(*_):
    global running
    running = False
    log.info("Shutdown requested...")


def fetch_city(session, city):
    """Return the parsed API response for a city, or None on failure."""
    params = {"key": config.WEATHER_API_KEY, "q": city, "aqi": "no"}
    try:
        resp = session.get(config.WEATHER_API_URL, params=params, timeout=10)
        if resp.status_code != 200:
            try:
                detail = resp.json().get("error", {}).get("message", resp.text)
            except ValueError:
                detail = resp.text
            log.error("API error for %s (HTTP %s): %s", city, resp.status_code, detail)
            return None
        return resp.json()
    except requests.RequestException as exc:
        log.error("Network error for %s: %s", city, exc)
        return None


def build_records(city, payload, timestamp):
    """Convert an API payload into schema-conformant records."""
    current = payload.get("current") or {}
    sensor_id = config.slug(city)
    records = []
    for metric_type, field in config.METRIC_FIELDS.items():
        value = current.get(field)
        if value is None:
            continue
        try:
            records.append(
                {
                    "timestamp": timestamp,
                    "value": float(value),
                    "metric_type": metric_type,
                    "sensor_id": sensor_id,
                }
            )
        except (TypeError, ValueError):
            log.warning("Skipping non-numeric %s for %s: %r", field, city, value)
    return records


def create_producer():
    while running:
        try:
            return KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=50,
            )
        except Exception as exc:
            log.warning("Kafka unavailable (%s). Retrying in 5s...", exc)
            time.sleep(5)
    return None


def main():
    if not config.WEATHER_API_KEY:
        sys.exit("WEATHER_API_KEY is missing. Copy .env.example to .env and set your key.")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info("Waiting for Kafka at %s ...", config.KAFKA_BOOTSTRAP)
    config.ensure_topic()
    producer = create_producer()
    if producer is None:
        return

    session = requests.Session()
    announced = set()
    log.info(
        "Streaming %d cities to topic '%s' every %ss",
        len(config.CITIES), config.KAFKA_TOPIC, config.FETCH_INTERVAL,
    )

    while running:
        cycle_start = time.monotonic()
        timestamp = datetime.now(timezone.utc).strftime(config.TS_FORMAT)
        sent = 0

        for city in config.CITIES:
            payload = fetch_city(session, city)
            if not payload:
                continue
            if city not in announced:
                loc = payload.get("location", {})
                log.info("'%s' resolved to %s, %s", city, loc.get("name"), loc.get("country"))
                announced.add(city)
            for record in build_records(city, payload, timestamp):
                producer.send(config.KAFKA_TOPIC, key=record["sensor_id"], value=record)
                sent += 1

        try:
            producer.flush(timeout=10)
            log.info("Published %d records @ %s", sent, timestamp)
        except Exception as exc:
            log.error("Flush failed: %s", exc)

        while running and time.monotonic() - cycle_start < config.FETCH_INTERVAL:
            time.sleep(0.5)

    producer.close(timeout=5)
    log.info("Producer stopped.")


if __name__ == "__main__":
    main()
