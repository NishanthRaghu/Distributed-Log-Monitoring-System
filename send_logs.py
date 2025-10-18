import os
import requests
import time
import random
import uuid

INGEST_URL = os.getenv("INGEST_URL", "http://ingestion:8000/ingest")
SERVICES = ["auth", "payments", "orders", "inventory", "frontend"]

def gen_log():
    return {
        "service": random.choice(SERVICES),
        "level": random.choice(["DEBUG", "INFO", "WARN", "ERROR"]),
        "message": f"Sample log {uuid.uuid4().hex[:6]}",
        "metadata": {"user_id": random.randint(1,1000)}
    }

if __name__ == "__main__":
    while True:
        log = gen_log()
        try:
            r = requests.post(INGEST_URL, json=log, timeout=5)
            print("Sent:", r.status_code, r.text)
        except Exception as e:
            print("Failed send:", e)
        time.sleep(float(os.getenv("SEND_INTERVAL", "2")))
