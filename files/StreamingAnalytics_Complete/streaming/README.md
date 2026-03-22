# Real-Time Streaming Analytics Pipeline

## Quick Start (No Kafka/Spark needed)

```bash
# Install dependencies
pip install streamlit plotly pandas pyyaml

# Run simulation mode (no Kafka/Spark required)
python consumers/spark_streaming.py

# Run tests
python tests/test_aggregations.py

# Run dashboard
streamlit run dashboard/app.py
```

## Full Stack (With Kafka + Spark)

```bash
# 1. Start Kafka (Docker)
docker run -d -p 9092:9092 --name kafka \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:latest

# 2. Start event producer
python producers/kafka_producer.py

# 3. Start Spark streaming job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  consumers/spark_streaming.py

# 4. Start dashboard
streamlit run dashboard/app.py
```

## Architecture

```
Event Producers → Kafka → PySpark Streaming → Delta Lake
                                    ↓
                           Anomaly Detector → Elasticsearch
                                    ↓
                           Streamlit Dashboard
```

## Team Split (5 Members)
| Member | Files |
|--------|-------|
| M1 | producers/event_generator.py + kafka_producer.py |
| M2 | consumers/aggregations.py |
| M3 | consumers/anomaly_detector.py |
| M4 | consumers/spark_streaming.py + sinks/ |
| M5 | dashboard/app.py + tests/ |
