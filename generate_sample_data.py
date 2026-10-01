"""
Assignment 2 - Sample Data Generator
Generates sample datasets for the 3 streaming sources + 1 static source
identified in Assignment 1 (Ride-Hailing / Urban Mobility pipeline).

Output files (in ./data/):
  - user_static_master.json     (static baseline - Topic 1)
  - ride_requests.json          (streaming     - Topic 2)
  - driver_location_feed.json   (streaming     - Topic 3, both zone clusters)
  - trip_status.json            (bonus stream, not in A1 topic list but referenced
                                  as a data source; folded into ride_requests
                                  downstream in the real design)
"""

import json
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

CITY_CORE_ZONES = ["Connaught Place", "MG Road", "Cyber City", "Saket", "Karol Bagh"]
SUBURBAN_ZONES = ["Manesar", "Sohna Road", "Dwarka Sector 21", "Faridabad NIT", "Bahadurgarh"]
VEHICLE_TYPES = ["Mini", "Sedan", "SUV", "Bike"]

def rand_latlong(base_lat, base_long, spread=0.05):
    return round(base_lat + random.uniform(-spread, spread), 6), \
           round(base_long + random.uniform(-spread, spread), 6)

# ---------------------------------------------------------------
# 1. STATIC MASTER DATA  -> Topic 1: user_static_master
# ---------------------------------------------------------------
def generate_user_static_master(n=20):
    records = []
    for i in range(1, n + 1):
        user_type = "driver" if i % 2 == 0 else "rider"  # even/odd -> matches A1 partitioning
        record = {
            "user_id": i,
            "user_type": user_type,
            "name": f"{'Driver' if user_type=='driver' else 'Rider'}_{i}",
            "phone": f"+91-9{random.randint(100000000, 999999999)}",
            "home_zone": random.choice(CITY_CORE_ZONES + SUBURBAN_ZONES),
            "kyc_verified": random.choice([True, True, True, False]),
            "vehicle_type": random.choice(VEHICLE_TYPES) if user_type == "driver" else None,
            "signup_date": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 500))).strftime("%Y-%m-%d"),
        }
        records.append(record)
    return records

# ---------------------------------------------------------------
# 2. RIDE REQUESTS -> Topic 2: ride_requests
# ---------------------------------------------------------------
def generate_ride_requests(n=30, rider_ids=None):
    records = []
    now = datetime.now()
    for i in range(n):
        pu_lat, pu_long = rand_latlong(28.6139, 77.2090)
        do_lat, do_long = rand_latlong(28.6139, 77.2090)
        record = {
            "request_id": str(uuid.uuid4())[:8],
            "rider_id": random.choice(rider_ids) if rider_ids else random.randint(1, 20),
            "pickup_lat": pu_lat,
            "pickup_long": pu_long,
            "dropoff_lat": do_lat,
            "dropoff_long": do_long,
            "fare_estimate": round(random.uniform(80, 650), 2),
            "vehicle_type_requested": random.choice(VEHICLE_TYPES),
            "timestamp": (now + timedelta(seconds=i * 4)).isoformat(),
            "batch": "A" if i % 2 == 0 else "B",  # matches A1 P0/P1 split
        }
        records.append(record)
    return records

# ---------------------------------------------------------------
# 3. DRIVER GPS PINGS -> Topic 3: driver_location_feed
# ---------------------------------------------------------------
def generate_driver_location_feed(n=40, driver_ids=None):
    records = []
    now = datetime.now()
    for i in range(n):
        zone_cluster = "A" if i % 2 == 0 else "B"  # City Core vs Suburban, matches A1 P0/P1
        base = (28.6139, 77.2090) if zone_cluster == "A" else (28.4089, 77.3178)
        lat, lng = rand_latlong(*base, spread=0.03)
        record = {
            "driver_id": random.choice(driver_ids) if driver_ids else random.randint(1, 20),
            "latitude": lat,
            "longitude": lng,
            "zone_cluster": zone_cluster,
            "zone_name": random.choice(CITY_CORE_ZONES if zone_cluster == "A" else SUBURBAN_ZONES),
            "status": random.choice(["available", "en_route", "on_trip"]),
            "speed_kmph": round(random.uniform(0, 60), 1),
            "timestamp": (now + timedelta(seconds=i * 3)).isoformat(),
        }
        records.append(record)
    return records

# ---------------------------------------------------------------
# 4. TRIP STATUS EVENTS (data source #3 from A1)
# ---------------------------------------------------------------
def generate_trip_status(n=25, request_ids=None):
    records = []
    now = datetime.now()
    statuses = ["trip_started", "trip_completed", "trip_cancelled"]
    for i in range(n):
        record = {
            "trip_id": str(uuid.uuid4())[:8],
            "request_id": random.choice(request_ids) if request_ids else str(uuid.uuid4())[:8],
            "status": random.choices(statuses, weights=[0.5, 0.4, 0.1])[0],
            "timestamp": (now + timedelta(seconds=i * 5)).isoformat(),
        }
        records.append(record)
    return records


if __name__ == "__main__":
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    users = generate_user_static_master(20)
    driver_ids = [u["user_id"] for u in users if u["user_type"] == "driver"]
    rider_ids = [u["user_id"] for u in users if u["user_type"] == "rider"]

    ride_requests = generate_ride_requests(30, rider_ids)
    request_ids = [r["request_id"] for r in ride_requests]

    driver_locations = generate_driver_location_feed(40, driver_ids)
    trip_status = generate_trip_status(25, request_ids)

    with open(os.path.join(data_dir, "user_static_master.json"), "w") as f:
        json.dump(users, f, indent=2)
    with open(os.path.join(data_dir, "ride_requests.json"), "w") as f:
        json.dump(ride_requests, f, indent=2)
    with open(os.path.join(data_dir, "driver_location_feed.json"), "w") as f:
        json.dump(driver_locations, f, indent=2)
    with open(os.path.join(data_dir, "trip_status.json"), "w") as f:
        json.dump(trip_status, f, indent=2)

    print(f"Generated {len(users)} user_static_master records")
    print(f"Generated {len(ride_requests)} ride_requests records")
    print(f"Generated {len(driver_locations)} driver_location_feed records")
    print(f"Generated {len(trip_status)} trip_status records")
    print("All files written to ./data/")
