# 🌦️ Weather Big Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-7-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.40-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

A real-time big data application that streams live weather readings from WeatherAPI through **Kafka** and **Spark Structured Streaming** into **MongoDB**, and presents them on an interactive **Streamlit** dashboard. It is built for anyone who wants to monitor current conditions across several Philippine cities, explore historical trends, and export formatted reports.



## Table of Contents

- [Features](#features)
- [Technology Stack](#technology-stack)
- [System Architecture](#system-architecture)
- [Getting Started](#getting-started)
- [API Endpoints](#api-endpoints)
- [Project Context](#project-context)



## Features

### 📡 Data Ingestion

- **Live API Polling:** fetches current weather for Manila, Cebu City, Davao City and Quezon City every 15 seconds.
- **Metric Splitting:** converts each response into four JSON messages (temperature, humidity, wind speed, pressure), producing 16 messages per cycle.
- **Resilient Producer:** retries broker connections, logs API errors and shuts down cleanly without losing queued messages.

### ⚙️ Stream Processing

- **Structured Streaming:** Spark continuously consumes the Kafka topic and parses and validates each record.
- **Windowed Aggregation:** computes one-minute average, minimum, maximum and count per city and metric.
- **Duplicate-Safe Writes:** upserts and unique indexes keep MongoDB consistent, even after a restart.

### 🗄️ Storage

- **Raw History:** every valid record is stored in the `weather_raw` collection.
- **Aggregate Store:** Spark results are stored in the `weather_agg` collection for fast analysis.

### 📊 Streamlit Dashboard

- **Real-Time View:** reads directly from Kafka and refreshes every 15 seconds with per-city cards, a time-series chart and a latest-readings table.
- **Historical View:** filters by city, metric and time range (preset or custom), with trend charts, summary statistics, Spark aggregates and a raw data table.
- **Formatted Export:** downloads the filtered data as CSV, a multi-sheet Excel workbook, a PDF report or a PNG chart.
- **Status Indicators:** shows live Kafka and MongoDB connection status in the sidebar.



## Technology Stack

| Layer | Technologies Used |
|---|---|
| Data Source | WeatherAPI (`current.json`) |
| Ingestion | Python, `kafka-python`, `requests` |
| Messaging | Apache Kafka 3.7 (Docker) |
| Processing | Apache Spark 3.5 (PySpark Structured Streaming), Java 17 |
| Storage | MongoDB 7 (Docker), PyMongo |
| Dashboard | Streamlit, Plotly, Pandas |
| Export | OpenPyXL, ReportLab, Matplotlib |
| Infrastructure | Docker Compose, Conda (Python 3.10) |



## System Architecture

1. **WeatherAPI** provides live weather data over HTTPS.
2. The **Kafka producer** (`producer.py`) polls the API every 15 seconds and publishes one JSON message per metric to the `weather_stream` topic.
3. **Kafka** buffers the stream and delivers it to its consumers.
4. **Spark Structured Streaming** (`spark_streaming.py`) reads the topic, then writes every raw record and the one-minute aggregates to MongoDB.
5. **MongoDB** stores the history in `weather_raw` and the aggregates in `weather_agg`.
6. The **Streamlit dashboard** (`app.py`) reads live data directly from Kafka and historical data from MongoDB, and generates the exports.

Message schema:

```json
{
  "timestamp": "2026-11-06T05:22:33Z",
  "value": 31.2,
  "metric_type": "temperature_c",
  "sensor_id": "cebu_city"
}
```



## Getting Started

### Prerequisites

- Ubuntu 22.04 or later (a VM with at least 4 GB of RAM is recommended)
- Docker and Docker Compose v2
- OpenJDK 17
- Miniconda (Python 3.10)
- A free [WeatherAPI](https://www.weatherapi.com/) API key

> **⚠️ Important:** You must provide your own WeatherAPI key in a `.env` file. Never commit `.env` or the `checkpoints/` folder. Add both to your `.gitignore`.

### 1. Install System Packages

```bash
sudo apt update
sudo apt install -y openjdk-17-jre-headless docker-compose-v2
sudo usermod -aG docker $USER
newgrp docker
```

### 2. Clone the Repository

```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
```

### 3. Create the Python Environment

```bash
conda create -n bigdata python=3.10.13 -y
conda activate bigdata
pip install -r requirements.txt
```

### 4. Configure the Environment

```bash
cp .env.example .env
nano .env
```

Set your key in `.env`:

```bash
WEATHER_API_KEY=your_api_key_here
```

### 5. Start Kafka and MongoDB

```bash
docker compose up -d
docker ps
```

Wait about 15 seconds until both `bd-kafka` and `bd-mongo` show `Up`.

### 6. Start the Spark Job

Open a new terminal, then run:

```bash
conda activate bigdata
python spark_streaming.py
```

Wait for the `Streaming started` message. The first run downloads the Kafka connector, so it needs an internet connection.

### 7. Start the Producer

Open a new terminal, then run:

```bash
conda activate bigdata
python producer.py
```

### 8. Launch the Dashboard

Open a new terminal, then run:

```bash
conda activate bigdata
streamlit run app.py
```

Open `http://localhost:8501` in your browser.

### 9. Stop or Reset

```bash
# Stop: press Ctrl+C in the Spark, producer and dashboard terminals, then
docker compose down

# Full reset (deletes stored data and Spark checkpoints)
docker compose down -v && rm -rf checkpoints/
```



## API Endpoints

This project has no backend API of its own. It consumes one external endpoint:

| Method | Endpoint | Description |
|---|---|---|
| GET | `https://api.weatherapi.com/v1/current.json?key=<KEY>&q=<CITY>&aqi=no` | Returns the current weather conditions for a city |



## Project Context

This project was built as a Big Data Application Pipeline activity. The goal was to implement a complete Kafka-Spark pipeline with MongoDB, using a public API as the data source, a live dashboard that updates every 15 seconds, full data history, aggregation and formatted exports.



<p align="center"><i>Built with Kafka, Spark, MongoDB and Streamlit, turning live weather data into insight.</i></p>
