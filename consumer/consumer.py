from kafka import KafkaConsumer
import json

# ---------------- Fleet Statistics ---------------- #

message_count = 0
overspeed_count = 0
low_fuel_count = 0
high_temp_count = 0

consumer = KafkaConsumer(
    "truck-data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="truck-monitor-group-v2",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("✅ Connected to Kafka. Waiting for truck data...\n")

try:
    for message in consumer:

        data = message.value
        message_count += 1

        alerts = []

        # ---------------- Alert Detection ---------------- #

        if data["fuel_level"] <= 20:
            alerts.append("⛽ LOW FUEL")
            low_fuel_count += 1

        if data["temperature"] >= 32:
            alerts.append("🔥 HIGH TEMPERATURE")
            high_temp_count += 1

        if data["speed"] >= 80:
            alerts.append("🚨 OVER SPEED")
            overspeed_count += 1

        # ---------------- Truck Information ---------------- #

        print("\n" + "=" * 70)
        print(f"🚚 {data['truck_id']} | Driver: {data['driver']}")
        print("-" * 70)
        print(f"🛣️ Route            : {data['route']}")
        print(f"📍 Current Location : {data['current_location']}")
        print(f"🚗 Speed            : {data['speed']} km/h")
        print(f"⛽ Fuel             : {data['fuel_level']}%")
        print(f"🌡️ Temperature      : {data['temperature']}°C")
        print(f"📊 Status           : {data['status']}")
        print(f"🕒 Timestamp        : {data['timestamp']}")
        print("=" * 70)

        # ---------------- Alerts ---------------- #

        if alerts:
            print("\n🚨 ACTIVE ALERTS")
            print("-" * 70)

            for alert in alerts:
                print(alert)

            print("=" * 70)

        # ---------------- Fleet Dashboard ---------------- #

        print("\n📊 LIVE FLEET DASHBOARD")
        print("-" * 70)
        print(f"📨 Messages Processed : {message_count}")
        print(f"🚨 Overspeed Alerts   : {overspeed_count}")
        print(f"⛽ Low Fuel Alerts    : {low_fuel_count}")
        print(f"🔥 High Temp Alerts   : {high_temp_count}")
        print("=" * 70)

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped.")

finally:
    consumer.close()