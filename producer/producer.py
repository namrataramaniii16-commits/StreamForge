from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
message = {
    "truck_id": "TRK001",
    "temperature": 24.5,
    "location": "Mumbai"
}

producer.send("truck-data", value=message)

producer.flush()

print("Message sent successfully!")