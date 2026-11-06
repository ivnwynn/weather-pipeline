# Weather Big Data Pipeline

**WeatherAPI → Kafka → Spark Structured Streaming → MongoDB → Streamlit**

| Layer | Tool | Role |
|---|---|---|
| Source | WeatherAPI (`current.json`) | Live weather for 4 Philippine cities |
| Ingestion | Kafka producer (`producer.py`) | One message per metric, every 15 s |
| Processing | Spark (`spark_streaming.py`) | Stores raw records + 1-minute windowed aggregates |
| Storage | MongoDB | `weather_raw` (history), `weather_agg` (aggregates) |
| Dashboard | Streamlit (`app.py`) | Live view (Kafka) + Historical view (MongoDB) + exports |

Message schema: `{"timestamp": ISO-8601 UTC, "value": float, "metric_type": str, "sensor_id": str}`

## Setup (Arch Linux)

```bash
# system packages
sudo pacman -S docker docker-compose jdk17-openjdk
sudo systemctl enable --now docker
sudo usermod -aG docker $USER      # log out and back in once

# python environment (miniconda, from AUR or the official installer)
conda create -n bigdata python=3.10.13 -y
conda activate bigdata
pip install -r requirements.txt

# configuration
cp .env.example .env               # then set WEATHER_API_KEY
```

## Run (4 terminals, project folder, `conda activate bigdata` in each)

```bash
docker compose up -d               # 1. Kafka + MongoDB
python spark_streaming.py          # 2. Spark job (first run downloads the Kafka connector)
python producer.py                 # 3. Producer
streamlit run app.py               # 4. Dashboard -> http://localhost:8501
```

## Troubleshooting

- **Reset everything:** `docker compose down -v && rm -rf checkpoints/`
- **Spark fails on Java version:** run `sudo archlinux-java set java-17-openjdk`
- **API key error:** check `WEATHER_API_KEY` in `.env`
- Spark UI: http://localhost:4040
- Never commit `.env` or `checkpoints/` (add both to `.gitignore`).
