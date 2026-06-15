import json
import time
import requests

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
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