"""
consumer_mysql.py
Assignment 3 — Ride-Hailing Dashboard

Consumes from Kafka topics and stores streaming data in MySQL.

Kafka Topics:
    1. user_static_master
    2. ride_requests
    3. driver_location_feed

Pipeline:

Kafka → Python Consumer → MySQL → Grafana
"""

import json
import mysql.connector

from kafka import KafkaConsumer


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

KAFKA_BROKER = "localhost:9092"

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "98@@@Aman30"
MYSQL_DATABASE = "ride_hailing"


# ---------------------------------------------------------------------------
# CONNECT TO MYSQL
# ---------------------------------------------------------------------------

db = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password="98@@@Aman30",
    database=MYSQL_DATABASE
)

cursor = db.cursor()

print("🗄️ MySQL connection successful")


# ---------------------------------------------------------------------------
# KAFKA CONSUMER
# ---------------------------------------------------------------------------

TOPICS = [
    "user_static_master",
    "ride_requests",
    "driver_location_feed"
]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    group_id="assignment3-mysql-group-v3",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)



# ---------------------------------------------------------------------------
# START MESSAGE
# ---------------------------------------------------------------------------

print()
print("=" * 70)
print("🚕 ASSIGNMENT 3 — RIDE HAILING SQL CONSUMER")
print("=" * 70)

print(f"Kafka   : {KAFKA_BROKER}")

print("Topics  :")
print("   • user_static_master")
print("   • ride_requests")
print("   • driver_location_feed")

print(f"MySQL   : {MYSQL_DATABASE}")

print("-" * 70)
print("Waiting for Kafka messages...")
print()


# ---------------------------------------------------------------------------
# MESSAGE COUNTER
# ---------------------------------------------------------------------------

count = 0


# ---------------------------------------------------------------------------
# CONSUME STREAM
# ---------------------------------------------------------------------------

for msg in consumer:

    record = msg.value

    topic = msg.topic

    partition = msg.partition

    offset = msg.offset


    try:

        # ================================================================
        # 1. USER STATIC MASTER
        # ================================================================

        if topic == "user_static_master":

            sql = """
                INSERT IGNORE INTO user_static_master
                (
                    user_id,
                    user_type,
                    home_zone,
                    kyc_status,
                    vehicle_type,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """

            values = (

                record.get("user_id"),

                record.get("user_type"),

                record.get("home_zone"),

                record.get("kyc_status")
                or record.get("KYC_status")
                or record.get("kyc"),

                record.get("vehicle_type"),

                topic,

                partition,

                offset
            )

            cursor.execute(sql, values)


            print(
                f"👤 USER  | "
                f"{record.get('user_id')} | "
                f"{record.get('user_type')} | "
                f"Zone: {record.get('home_zone')}"
            )


        # ================================================================
        # 2. RIDE REQUESTS
        # ================================================================

        elif topic == "ride_requests":

            sql = """
                INSERT IGNORE INTO ride_requests
                (
                    ride_request_id,
                    rider_id,

                    pickup_lat,
                    pickup_long,

                    dropoff_lat,
                    dropoff_long,

                    fare_estimate,

                    requested_vehicle_type,
                    batch,

                    event_timestamp,

                    kafka_topic,
                    kafka_partition,
                    kafka_offset
                )
                VALUES
                (
                    %s,%s,
                    %s,%s,
                    %s,%s,
                    %s,
                    %s,%s,
                    %s,
                    %s,%s,%s
                )
            """

            values = (

                record.get("ride_request_id")
                or record.get("request_id")
                or record.get("ride_id"),

                record.get("rider_id"),

                record.get("pickup_lat")
                or record.get("pickup_latitude"),

                record.get("pickup_long")
                or record.get("pickup_lng")
                or record.get("pickup_longitude"),

                record.get("dropoff_lat")
                or record.get("dropoff_latitude"),

                record.get("dropoff_long")
                or record.get("dropoff_lng")
                or record.get("dropoff_longitude"),

                record.get("fare_estimate")
                or record.get("estimated_fare"),

                record.get("requested_vehicle_type")
                or record.get("vehicle_type"),

                record.get("batch"),

                record.get("timestamp")
                or record.get("event_timestamp"),

                topic,

                partition,

                offset
            )

            cursor.execute(sql, values)


            print(
                f"🚕 RIDE  | "
                f"{record.get('ride_request_id') or record.get('request_id')} | "
                f"Batch: {record.get('batch')} | "
                f"Fare: ₹{record.get('fare_estimate')}"
            )


        # ================================================================
        # 3. DRIVER LOCATION FEED
        # ================================================================

        elif topic == "driver_location_feed":

            sql = """
                INSERT IGNORE INTO driver_location_feed
                (
                    driver_id,
                    latitude,
                    longitude,
                    driver_status,
                    speed,
                    zone_cluster,
                    event_timestamp,

                    kafka_topic,
                    kafka_partition,
                    kafka_offset
                )
                VALUES
                (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
            """

            values = (

                record.get("driver_id"),

                record.get("latitude")
                or record.get("lat"),

                record.get("longitude")
                or record.get("long")
                or record.get("lng"),

                record.get("driver_status")
                or record.get("status"),

                record.get("speed"),

                record.get("zone_cluster"),

                record.get("timestamp")
                or record.get("event_timestamp"),

                topic,

                partition,

                offset
            )

            cursor.execute(sql, values)


            print(
                f"📍 DRIVER | "
                f"{record.get('driver_id')} | "
                f"Zone: {record.get('zone_cluster')} | "
                f"Status: {record.get('driver_status') or record.get('status')}"
            )


        # ================================================================
        # COMMIT TO MYSQL
        # ================================================================

        db.commit()

        count += 1

        print(
            f"   💾 Saved to MySQL | "
            f"Partition: {partition} | "
            f"Offset: {offset}"
        )

        print("-" * 70)


    except Exception as e:

        print()
        print("❌ ERROR")
        print(f"Topic     : {topic}")
        print(f"Partition : {partition}")
        print(f"Offset    : {offset}")
        print(f"Error     : {e}")
        print("-" * 70)