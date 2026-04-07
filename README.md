# Real-Time Feature Store and Fraud Detection System

## Overview
This project is a simulation of a Real-Time Feature Store architecture built for a Fraud Detection System. It ingests simulated transaction data, processes it in real-time to compute user features (e.g., transaction counts and total spending within a time window), and serves these features with low latency to make instant Approve or Reject decisions. It also stores historical data for offline analysis.

## Architecture & Technologies
- **Apache Kafka**: Message broker for streaming transaction data in real-time.
- **Apache Flink (PyFlink)**: Stream processing engine used to aggregate sliding/tumbling window features on the fly.
- **Redis**: In-memory data store acting as the Online Feature Store for ultra-low latency feature retrieval.
- **Apache Iceberg**: Open table format used as the Offline Feature Store / Data Warehouse for historical data storage.
- **Docker & Docker Compose**: Containerization for running infrastructure components (Kafka, Redis, Zookeeper).
- **Python**: Core programming language.

## Workflow Description
1. **Data Ingestion**: `transaction_producer.py` generates synthetic transaction data and publishes it to a Kafka topic (`transactions`).
2. **Real-time Processing**: `flink_processor.py` consumes the Kafka stream, calculates aggregations (1-minute windowed total spending and transaction count), and updates the online features in Redis.
3. **Fraud Detection (Decision Engine)**: `fraud_detector.py` simulates incoming transaction requests, retrieves the latest user features from Redis, and evaluates them against fraud rules to output an `APPROVE` or `REJECT` decision.
4. **Offline Storage**: `iceberg_consumer.py` consumes the same transactions and writes them into an Apache Iceberg table (stored locally in the `warehouse/` directory) for future batch analytics, model training, or historical auditing.

## Project Structure
```text
realtime-feature-store/
├── .venv/                      # Python virtual environment
├── lib/                        # Contains Flink-Kafka connector JAR files
├── warehouse/                  # Apache Iceberg offline data storage
├── docker-compose.yml          # Infrastructure setup (Kafka, Redis)
├── flink_processor.py          # PyFlink script for real-time feature engineering
├── fraud_detector.py           # Simulation script for fraud decision making
├── iceberg_consumer.py         # Script to sink Kafka data to Apache Iceberg
├── requirements.txt            # Python dependencies
└── transaction_producer.py     # Kafka producer generating dummy transactions
```

## Setup and Installations

### 1. Prerequisites
- **Python 3.8+** installed on your host machine.
- **Docker Desktop** or Docker Compose installed and running.
- **Java 11/8** (required for running Apache Flink).

### 2. Configuration
1. Clone the repository and navigate to the project directory:
   ```bash
   git clone <repository_url>
   cd realtime-feature-store
   ```
2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # Linux/macOS
   source .venv/bin/activate
   ```
3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Download the Flink Kafka Connector JAR:
   Download `flink-sql-connector-kafka-3.1.0-1.18.jar` (or the version matching your Flink setup) and place it in the `lib/` folder.

### 3. Running the Platform
Run the following components in separate terminal windows:

**Step A: Start Infrastructure**
```bash
docker-compose up -d
```

**Step B: Start Data Producer**
```bash
python transaction_producer.py
```

**Step C: Start Offline Storage Consumer (Iceberg)**
```bash
python iceberg_consumer.py
```

**Step D: Start Real-time Feature Engineering (Flink to Redis)**
```bash
python flink_processor.py
```

**Step E: Start Fraud Detection Engine**
```bash
python fraud_detector.py
```
