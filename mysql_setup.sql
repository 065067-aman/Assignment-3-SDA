CREATE DATABASE IF NOT EXISTS ride_hailing;

USE ride_hailing;

-- ==========================================
-- 1. USER STATIC MASTER
-- ==========================================

CREATE TABLE IF NOT EXISTS user_static_master (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    user_type VARCHAR(30),
    home_zone VARCHAR(100),
    kyc_status VARCHAR(30),
    vehicle_type VARCHAR(50),

    kafka_topic VARCHAR(100),
    kafka_partition INT,
    kafka_offset BIGINT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY unique_kafka_record
    (kafka_topic, kafka_partition, kafka_offset),

    INDEX idx_user_id (user_id),
    INDEX idx_home_zone (home_zone)
);


-- ==========================================
-- 2. RIDE REQUESTS
-- ==========================================

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
    (kafka_topic, kafka_partition, kafka_offset),

    INDEX idx_batch (batch),
    INDEX idx_timestamp (event_timestamp)
);


-- ==========================================
-- 3. DRIVER LOCATION FEED
-- ==========================================

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
    (kafka_topic, kafka_partition, kafka_offset),

    INDEX idx_driver_id (driver_id),
    INDEX idx_zone (zone_cluster),
    INDEX idx_status (driver_status),
    INDEX idx_timestamp (event_timestamp)
);


-- ==========================================
-- 4. TRIP STATUS
-- ==========================================
-- Assignment 2 generated trip_status.json,
-- but it was not created as a Kafka topic.
-- Therefore we don't consume it here unless
-- you later create a Kafka topic for it.

CREATE TABLE IF NOT EXISTS trip_status (
    id INT AUTO_INCREMENT PRIMARY KEY,

    trip_id VARCHAR(100),
    ride_request_id VARCHAR(100),
    status VARCHAR(50),

    event_timestamp DATETIME NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);