"""Shared configuration and helpers for the producer, Spark job and dashboard."""
import os
import time

from dotenv import load_dotenv

load_dotenv()


def _csv(name, default):
    return [x.strip() for x in os.getenv(name, default).split(",") if x.strip()]


# --- Weather API ---------------------------------------------------------
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "").strip()
WEATHER_API_URL = "https://api.weatherapi.com/v1/current.json"
CITIES = _csv("CITIES", "Manila,Cebu City,Davao City,Quezon City")
FETCH_INTERVAL = max(5, int(os.getenv("FETCH_INTERVAL", "15")))


def slug(city):
    return city.strip().lower().replace(" ", "_")


SENSORS = [slug(c) for c in CITIES]

# metric_type -> field name inside the API's "current" object
METRIC_FIELDS = {
    "temperature_c": "temp_c",
    "humidity_pct": "humidity",
    "wind_kph": "wind_kph",
    "pressure_mb": "pressure_mb",
}
METRIC_LABELS = {
    "temperature_c": ("Temperature", "°C"),
    "humidity_pct": ("Humidity", "%"),
    "wind_kph": ("Wind Speed", "km/h"),
    "pressure_mb": ("Pressure", "mb"),
}

# --- Kafka ---------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_stream")
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"  # ISO 8601, UTC

# --- MongoDB -------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "weather_db")
RAW_COLLECTION = "weather_raw"
AGG_COLLECTION = "weather_agg"

# --- Spark / dashboard ---------------------------------------------------
AGG_WINDOW = os.getenv("AGG_WINDOW", "1 minute")
REFRESH_SECONDS = max(5, int(os.getenv("REFRESH_SECONDS", "15")))
DISPLAY_TZ = os.getenv("DISPLAY_TZ", "Asia/Manila")
BUFFER_SIZE = 5000


def sensor_label(sensor_id):
    return sensor_id.replace("_", " ").title()


def metric_label(metric_type):
    name, unit = METRIC_LABELS.get(metric_type, (metric_type, ""))
    return f"{name} ({unit})" if unit else name


def metric_unit(metric_type):
    return METRIC_LABELS.get(metric_type, (metric_type, ""))[1]


def ensure_topic(retries=10, delay=3):
    """Create the Kafka topic if it does not exist yet (waits for the broker)."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    last_error = None
    for _ in range(retries):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
            try:
                admin.create_topics(
                    [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
                )
            except TopicAlreadyExistsError:
                pass
            finally:
                admin.close()
            return
        except Exception as exc:  # broker not ready yet
            last_error = exc
            time.sleep(delay)
    raise RuntimeError(f"Kafka not reachable at {KAFKA_BOOTSTRAP}: {last_error}")
