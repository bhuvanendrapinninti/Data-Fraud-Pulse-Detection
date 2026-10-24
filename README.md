# DataPulse — Real-Time Fraud Detection Data Pipeline

Educational data engineering project: Python producer → Kafka → PySpark Structured Streaming → PostgreSQL → Streamlit.

This project uses synthetic data and simple rules. It is not a real banking fraud detector, and flagged transactions are not confirmed fraud.

## Prerequisites
- Windows 11 with Docker Desktop running Linux containers
- Internet connection for Docker images and the Spark Kafka connector
- 8 GB RAM recommended for Docker Desktop

Spark runs inside Linux containers to avoid Windows Java/Hadoop setup complications.

## Run
Open PowerShell in this folder:

```powershell
docker compose up --build -d
docker compose ps
docker compose logs -f processor
```

Open http://localhost:8501.

Watch generated events:

```powershell
docker compose logs -f producer
```

Query PostgreSQL:

```powershell
docker compose exec postgres psql -U frauduser -d frauddb -c "SELECT COUNT(*) FROM transactions;"
docker compose exec postgres psql -U frauduser -d frauddb -c "SELECT transaction_id, customer_id, amount, risk_reasons FROM transactions WHERE is_suspicious ORDER BY event_time DESC LIMIT 10;"
docker compose exec postgres psql -U frauduser -d frauddb -c "SELECT COUNT(*) FROM fraud_alerts;"
```

Stop services and preserve data:

```powershell
docker compose down
```

Reset all database/checkpoint data (destructive):

```powershell
docker compose down -v
```

## Key design choices
- `transaction_id` is a PostgreSQL primary key.
- `ON CONFLICT (transaction_id)` makes database writes idempotent across retries.
- Spark checkpoint files persist source progress and streaming state. Keep the volume for normal restarts.
- High-amount rule: amount >= `FRAUD_AMOUNT_THRESHOLD` (default ₹50,000).
- Kafka containers use `kafka:29092`; host applications use `localhost:9092`.
- Single-broker Kafka is for local development, not high availability.
- Local demo credentials are not safe for deployment.

## Troubleshooting
- **No data:** inspect `docker compose logs -f producer` and `docker compose logs -f processor`; wait up to a minute.
- **Spark connector download fails:** processor downloads `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` from Maven on first start; check internet access and restart it.
- **Kafka connection failure:** use `kafka:29092` inside Compose and `localhost:9092` on Windows.
- **Postgres connection failure:** run `docker compose ps` and `docker compose logs postgres`.
- **Port conflict:** free or remap host ports 5432, 8501, or 9092.
- **SQL changes not applied:** init SQL only runs when the database volume is first created; apply changes manually or reset the dev volume.

## Interview topics
Kafka partitions and offsets; Spark micro-batches and checkpoints; event time and watermarks; idempotent writes; schema validation; retries; throughput and latency measurement.

## Extensions
Add a dead-letter topic for malformed records, rapid-transaction window alerts, Airflow reconciliation, integration tests, and ML evaluation using a labeled dataset.
