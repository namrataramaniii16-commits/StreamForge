from kafka import KafkaProducer
from truck_data import TRUCKS, LOCATIONS

import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("✅ Connected to Kafka successfully!\n")

try:
    while True:

        truck = random.choice(TRUCKS)

        message = {
            "truck_id": truck["truck_id"],
            "driver": truck["driver"],
            "route": truck["route"],
            "current_location": random.choice(LOCATIONS),
            "speed": random.randint(40, 90),
            "fuel_level": random.randint(20, 100),
            "temperature": round(random.uniform(18.0, 35.0), 2),
            "status": truck["status"],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send("truck-data", value=message)
        producer.flush()

        print("\n" + "=" * 70)
        print(f"🚚 {message['truck_id']} | Driver: {message['driver']}")
        print("-" * 70)
        print(f"🛣️ Route            : {message['route']}")
        print(f"📍 Current Location : {message['current_location']}")
        print(f"🚗 Speed            : {message['speed']} km/h")
        print(f"⛽ Fuel             : {message['fuel_level']}%")
        print(f"🌡️ Temperature      : {message['temperature']}°C")
        print(f"📊 Status           : {message['status']}")
        print(f"🕒 Timestamp        : {message['timestamp']}")
        print("=" * 70)

        time.sleep(2)

except KeyboardInterrupt:
    print("\n🛑 Producer stopped by user.")

finally:
    producer.close()