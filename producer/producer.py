from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Connected to Kafka successfully!\n")

# 50 trucks
trucks = [f"TRK{str(i).zfill(3)}" for i in range(1, 51)]

# Truck locations
locations = [
    "Mumbai", "Pune", "Nagpur", "Nashik", "Thane",
    "Aurangabad", "Kolhapur", "Solapur", "Amravati", "Jalgaon"
]

try:
    while True:
        message = {
            "truck_id": random.choice(trucks),
            "temperature": round(random.uniform(18.0, 35.0), 2),
            "location": random.choice(locations),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send("truck-data", value=message)
        producer.flush()

        print("=" * 60)
        print("Truck Data Sent")
        print(f"Truck ID    : {message['truck_id']}")
        print(f"Temperature : {message['temperature']} °C")
        print(f"Location    : {message['location']}")
        print(f"Timestamp   : {message['timestamp']}")
        print("=" * 60)

        time.sleep(2)

except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.close()