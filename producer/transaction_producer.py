import json, os, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
RATE = max(1, int(os.getenv("EVENTS_PER_SECOND", "5")))
FRAUD_RATE = min(1.0, max(0.0, float(os.getenv("FRAUD_RATE", "0.08"))))

def make_transaction():
    high = random.random() < FRAUD_RATE
    amount = round(random.uniform(50001, 150000), 2) if high else round(random.uniform(50, 12000), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1,100):04d}",
        "amount": amount, "currency": "INR",
        "merchant_id": f"MER-{random.randint(1,30):03d}",
        "event_type": "payment",
        "event_time": datetime.now(timezone.utc).isoformat()
    }

def main():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode(), acks="all", retries=10
            )
            break
        except NoBrokersAvailable:
            print("Kafka not ready; retrying...", flush=True)
            time.sleep(3)
    print(f"Producing synthetic events to {TOPIC}", flush=True)
    try:
        while True:
            event = make_transaction()
            producer.send(TOPIC, key=event["customer_id"], value=event)
            producer.flush()
            print(f"sent {event['transaction_id']} amount={event['amount']}", flush=True)
            time.sleep(1 / RATE)
    finally:
        producer.close()

if __name__ == "__main__":
    main()
