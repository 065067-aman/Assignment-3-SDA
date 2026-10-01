import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

zones = ["Zone A", "Zone B"]
vehicle_types = ["Sedan", "SUV", "Hatchback"]

driver_ids = [f"D{i:03d}" for i in range(1, 11)]

print("LIVE STREAMING STARTED...")
print("Press Ctrl+C to stop.\n")

try:
    while True:

        # -----------------------------
        # 1. Generate a new ride request
        # -----------------------------
        ride_id = f"R{random.randint(1000, 9999)}"
        rider_id = f"U{random.randint(1, 20):03d}"

        ride_request = {
            "ride_request_id": ride_id,
            "rider_id": rider_id,
            "pickup_latitude": round(random.uniform(28.35, 28.55), 6),
            "pickup_longitude": round(random.uniform(76.85, 77.15), 6),
            "dropoff_latitude": round(random.uniform(28.35, 28.55), 6),
            "dropoff_longitude": round(random.uniform(76.85, 77.15), 6),
            "fare_estimate": random.randint(200, 600),
            "requested_vehicle_type": random.choice(vehicle_types),
            "batch": random.choice(["A", "B"]),
            "event_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send(
            "ride_requests",
            key=ride_id.encode("utf-8"),
            value=ride_request
        )

        print(f"[RIDE] {ride_id} | Fare: ₹{ride_request['fare_estimate']}")

        # -----------------------------
        # 2. Update driver locations
        # -----------------------------
        for driver_id in random.sample(driver_ids, 2):

            driver_update = {
                "driver_id": driver_id,
                "latitude": round(random.uniform(28.35, 28.55), 6),
                "longitude": round(random.uniform(76.85, 77.15), 6),
                "driver_status": random.choice(
                    ["available", "en_route", "on_trip"]
                ),
                "speed": random.randint(10, 60),
                "zone_cluster": random.choice(zones),
                "event_timestamp": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            }

            producer.send(
                "driver_location_feed",
                key=driver_id.encode("utf-8"),
                value=driver_update
            )

            print(
                f"[DRIVER] {driver_id} | "
                f"{driver_update['driver_status']} | "
                f"{driver_update['zone_cluster']}"
            )

        producer.flush()

        print("-" * 60)

        # Wait before next streaming cycle
        time.sleep(3)

except KeyboardInterrupt:
    print("\nLive streaming stopped.")

finally:
    producer.close()