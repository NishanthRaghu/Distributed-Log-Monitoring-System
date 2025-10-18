import os
import json
import time
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topic = os.getenv("LOG_TOPIC", "logs")
group = os.getenv("CONSUMER_GROUP", "log-consumers")
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
db_name = os.getenv("MONGO_DB", "logs_db")
collection_name = os.getenv("MONGO_COLLECTION", "logs")

conf = {
    "bootstrap.servers": bootstrap,
    "group.id": group,
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
consumer.subscribe([topic])

client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
db = client[db_name]
collection = db[collection_name]

print("Consumer started, subscribed to:", topic)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print("Error:", msg.error())
                continue
        try:
            payload = json.loads(msg.value().decode("utf-8"))
            payload["_received_at"] = time.time()
            # Insert into MongoDB (idempotency considerations omitted for clarity)
            collection.insert_one(payload)
            # Optionally print summary
            print(f"Saved log id={payload.get('id')} from {payload.get('service')}")
        except Exception as e:
            print("Failed to process message:", e)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    client.close()
