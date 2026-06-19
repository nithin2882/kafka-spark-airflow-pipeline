import json
import time
import requests
import os

from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

print("Kafka Server:", KAFKA_BOOTSTRAP_SERVERS)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "users_topic"

while True:

    response = requests.get(
        "https://randomuser.me/api/"
    )

    user = response.json()["results"][0]

    data = {
        "first_name": user["name"]["first"],
        "last_name": user["name"]["last"],
        "email": user["email"],
        "country": user["location"]["country"]
    }

    producer.send(
        TOPIC,
        value=data
    )

    producer.flush()

    print("Produced:", data)

    time.sleep(5)