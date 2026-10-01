# 🚕 Ride-Hailing Real-Time Analytics Dashboard

A real-time ride-hailing analytics pipeline built using **Apache Kafka, Python, MySQL, and Grafana**. The project streams ride and driver data through Kafka, consumes and stores the events in MySQL, and visualizes key operational metrics through an interactive Grafana dashboard.

## 🏗️ Architecture

```text
Ride-Hailing Data
       │
       ▼
 Apache Kafka
       │
       ├── user_static_master
       ├── ride_requests
       └── driver_location_feed
       │
       ▼
 Python Kafka Consumer
       │
       ▼
     MySQL
       │
       ▼
    Grafana
       │
       ▼
Real-Time Analytics Dashboard
```

## 🛠️ Tech Stack

* **Apache Kafka** – Real-time event streaming
* **Python** – Kafka consumer and data processing
* **MySQL** – Storage of consumed Kafka events
* **Grafana** – Dashboard and data visualization
* **Docker** – Kafka and supporting services
* **VS Code** – Development environment

## 📊 Dashboard Features

The Grafana dashboard provides:

### Key Performance Indicators

| KPI                 | Value |
| ------------------- | ----: |
| Total Ride Requests |    30 |
| Total Drivers       |    10 |
| Average Fare        |  ₹345 |
| Active Drivers      |     8 |

### Visualizations

* **Ride Requests by Batch**

  * Batch A: 15
  * Batch B: 15
* **Current Driver Supply by Zone**

  * Zone A: 40%
  * Zone B: 60%
* **Average Fare by Batch**
* **Current Driver Status**

  * Available: 40%
  * En-route: 40%
  * On-trip: 20%

## 📁 Kafka Topics

The project uses three Kafka topics:

```text
user_static_master
ride_requests
driver_location_feed
```

### `user_static_master`

Stores static customer information such as:

* User ID
* User type
* Home zone
* KYC status
* Vehicle type

### `ride_requests`

Contains ride request events including:

* Ride request ID
* Rider ID
* Pickup location
* Drop-off location
* Estimated fare
* Requested vehicle type
* Batch
* Event timestamp

### `driver_location_feed`

Contains driver telemetry including:

* Driver ID
* Latitude
* Longitude
* Driver status
* Speed
* Zone cluster
* Event timestamp

## 🔄 Data Flow

```text
Kafka Topics
     ↓
Python Kafka Consumer
     ↓
MySQL Tables
     ↓
Grafana SQL Queries
     ↓
Dashboard
```

The Python consumer reads events from Kafka and stores them in corresponding MySQL tables. Grafana then queries MySQL using SQL to generate the dashboard KPIs and visualizations.

## 📌 Business Insights

The dashboard provides operational visibility into ride demand, driver supply, pricing, and driver availability.

* Driver supply is distributed unevenly across zones, with **60% in Zone B and 40% in Zone A**.
* **8 out of 10 drivers** are currently available or en-route.
* The dataset contains **30 ride requests** with an average estimated fare of **₹345**.
* Comparing demand, driver supply, and fare patterns can help managers monitor operational conditions and support decisions related to driver allocation and supply management.

## 📂 Project Structure

```text
Assignment 3/
│
├── consumer_mysql.py
├── create_tables.py
├── user_static_master.json
├── ride_requests.json
├── driver_location_feed.json
├── README.md
│
└── screenshots/
    ├── kafka_producer.png
    ├── mysql_data.png
    └── grafana_dashboard.png
```

## 🚀 How to Run

### 1. Start Kafka and supporting services

Start the Docker containers containing Kafka and Zookeeper.

### 2. Create the MySQL database

Create the database:

```sql
CREATE DATABASE ride_hailing;
```

### 3. Create MySQL tables

Run:

```bash
python create_tables.py
```

### 4. Start the Kafka producer

Stream the ride-hailing datasets into the Kafka topics.

### 5. Start the MySQL consumer

Run:

```bash
python consumer_mysql.py
```

The consumer reads Kafka events and inserts them into MySQL.

### 6. Configure Grafana

Connect Grafana to the MySQL database:

```text
Database: ride_hailing
Host: localhost:3306
```

Then create dashboard panels using SQL queries.

## 📈 Example SQL Query

### Ride Requests by Batch

```sql
SELECT batch, COUNT(*) AS ride_requests
FROM ride_requests
GROUP BY batch
ORDER BY batch;
```

### Current Driver Supply by Zone

```sql
SELECT zone_cluster, COUNT(*) AS driver_supply
FROM (
    SELECT driver_id, zone_cluster,
           ROW_NUMBER() OVER (
               PARTITION BY driver_id
               ORDER BY created_at DESC, id DESC
           ) AS rn
    FROM driver_location_feed
) latest
WHERE rn = 1
GROUP BY zone_cluster
ORDER BY driver_supply DESC;
```

## 🎯 Project Objective

The objective is to demonstrate a **real-time streaming analytics pipeline** for a ride-hailing use case. The system captures streaming events through Kafka, stores consumed data in MySQL, and provides operational insights through a Grafana dashboard.

## ⚠️ Limitations

* The current implementation uses a sample dataset rather than continuous production traffic.
* Dashboard metrics are based on the available streamed records.
* Automated surge pricing, ride-driver matching, and ETA calculation are part of the broader proposed architecture but are **not implemented as automated features in this dashboard**.

---

### 👨‍💻 Project

**Ride-Hailing Real-Time Analytics Dashboard**

**Technology:** `Kafka → Python → MySQL → Grafana`

**Purpose:** Real-time monitoring of ride demand, driver supply, fare patterns, and fleet status.
