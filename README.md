# 🚕 Ride-Hailing Real-Time Streaming Analytics Dashboard

A real-time ride-hailing analytics pipeline built using **Apache Kafka, Python, MySQL, and Grafana**.

The project continuously generates ride and driver events, streams them through Kafka, consumes the events using Python, stores them in MySQL, and visualizes the latest operational metrics through an auto-refreshing Grafana dashboard.

---

## 📌 Project Overview

This project demonstrates a real-time streaming analytics pipeline for a ride-hailing platform.

The system continuously produces:

- Ride request events
- Driver location updates
- Driver status updates
- Zone information
- Fare estimates

These events are streamed through **Apache Kafka**, consumed by a Python application, stored in **MySQL**, and visualized using **Grafana**.

The Grafana dashboard automatically refreshes to display the latest streaming data.

---

## 🏗️ System Architecture

```text
              LIVE DATA GENERATOR
                     │
                     ▼
              Apache Kafka
                     │
          ┌──────────┴──────────┐
          │                     │
    ride_requests       driver_location_feed
          │                     │
          └──────────┬──────────┘
                     ▼
             Python Consumer
                     │
                     ▼
                  MySQL
                     │
                     ▼
                 Grafana
                     │
                     ▼
        Real-Time Analytics Dashboard
