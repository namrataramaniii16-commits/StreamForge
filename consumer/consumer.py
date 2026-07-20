from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "truck-data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="truck-monitor-group-v2",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Connected to Kafka. Waiting for truck data...\n")

try:
    for message in consumer:

        data = message.value

        print(data)

        print("=" * 60)
        print("🚚 Truck Data Received")
        print(f"Truck ID         : {data['truck_id']}")
        print(f"Driver           : {data['driver']}")
        print(f"Route            : {data['route']}")
        print(f"Current Location : {data['current_location']}")
        print(f"Speed            : {data['speed']} km/h")
        print(f"Fuel Level       : {data['fuel_level']}%")
        print(f"Temperature      : {data['temperature']}°C")
        print(f"Status           : {data['status']}")
        print(f"Timestamp        : {data['timestamp']}")
        print("=" * 60)

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()