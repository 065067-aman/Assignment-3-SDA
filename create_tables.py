import mysql.connector

# ------------------------------------------------------------
# MYSQL CONNECTION
# ------------------------------------------------------------

db = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="98@@@Aman30",
    database="ride_hailing"
)

cursor = db.cursor()

print("Connected to MySQL")


# ------------------------------------------------------------
# TABLE 1 — USER STATIC MASTER
# ------------------------------------------------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_static_master (

    id INT AUTO_INCREMENT PRIMARY KEY,

    user_id VARCHAR(50),

    user_type VARCHAR(50),

    home_zone VARCHAR(100),

    kyc_status VARCHAR(50),

    vehicle_type VARCHAR(50),

    kafka_topic VARCHAR(100),

    kafka_partition INT,

    kafka_offset BIGINT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY unique_kafka_record
    (
        kafka_topic,
        kafka_partition,
        kafka_offset
    )
)
""")

print("✓ user_static_master table ready")


# ------------------------------------------------------------
# TABLE 2 — RIDE REQUESTS
# ------------------------------------------------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS ride_requests (

    id INT AUTO_INCREMENT PRIMARY KEY,

    ride_request_id VARCHAR(100),

    rider_id VARCHAR(100),

    pickup_lat DECIMAL(10,7),

    pickup_long DECIMAL(10,7),

    dropoff_lat DECIMAL(10,7),

    dropoff_long DECIMAL(10,7),

    fare_estimate DECIMAL(10,2),

    requested_vehicle_type VARCHAR(50),

    batch VARCHAR(10),

    event_timestamp DATETIME NULL,

    kafka_topic VARCHAR(100),

    kafka_partition INT,

    kafka_offset BIGINT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY unique_kafka_record
    (
        kafka_topic,
        kafka_partition,
        kafka_offset
    )
)
""")

print("✓ ride_requests table ready")


# ------------------------------------------------------------
# TABLE 3 — DRIVER LOCATION FEED
# ------------------------------------------------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS driver_location_feed (

    id INT AUTO_INCREMENT PRIMARY KEY,

    driver_id VARCHAR(100),

    latitude DECIMAL(10,7),

    longitude DECIMAL(10,7),

    driver_status VARCHAR(50),

    speed DECIMAL(10,2),

    zone_cluster VARCHAR(50),

    event_timestamp DATETIME NULL,

    kafka_topic VARCHAR(100),

    kafka_partition INT,

    kafka_offset BIGINT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY unique_kafka_record
    (
        kafka_topic,
        kafka_partition,
        kafka_offset
    )
)
""")

print("✓ driver_location_feed table ready")


# ------------------------------------------------------------
# COMMIT
# ------------------------------------------------------------

db.commit()

print()
print("=" * 60)
print("ALL 3 TABLES CREATED SUCCESSFULLY")
print("=" * 60)


cursor.close()
db.close()