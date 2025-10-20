# ✈️ The OpenSky Network — Data Pipeline with Airflow, Spark & ClickHouse

> End-to-end **ETL pipeline** that collects, processes, and stores real-time flight data  
> using **Apache Airflow**, **PostgreSQL**, **Apache Spark**, and **ClickHouse** — all inside **Docker** 🚀

---

### 🛰️ Data Flow

**OpenSky API → 🪶 Airflow → 🐘 PostgreSQL → ⚡ Spark → 🏭 ClickHouse**

| Component             | Role                                            |
| --------------------- | ----------------------------------------------- |
| 🪶 **Airflow**        | Orchestrates DAGs for API ingestion & data flow |
| ⚡ **Spark**          | Transforms and processes flight data            |
| 🏭 **ClickHouse**     | Stores processed data for analytics             |
| 🐘 **PostgreSQL**     | Used by Airflow as metadata & staging DB        |
| 🐳 **Docker Compose** | Manages the entire local environment            |
| 🐍 **Python 3.10+**   | Language                                        |

---

## 🧱 Project Structure

```bash
.
├── dags/                          # Airflow DAGs: orchestrate data workflow
│   ├── create_clickhouse_tables.py
│   ├── spark_to_clickhouse.py
│   └── ...
│
├── spark/
│   ├── Dockerfile                 # Spark master & worker image
│   ├── spark_to_clickhouse.py     # Spark transformation script
│   └── work/                      # temp folder (gitignored)
│
├── shared_data/                   # shared volume between Airflow & Spark
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

## ⚙️ Prerequisites

Before running the project, make sure you have:

🐳 Docker

🧩 Docker Compose

💾 At least 8 GB RAM

## 🏗️ Setup & Run

1️⃣ Build and start all containers

`docker-compose up -d --build`

2️⃣ Initialize Airflow

`docker-compose run airflow-init`

Then restart all services:

`docker-compose up -d`

3️⃣ Verify containers are running

`docker ps`

## 🌐 Web Interfaces

Service URL Description

🪶 Airflow Web UI http://localhost:8082 DAG management & monitoring

⚡ Spark Master UI http://localhost:8080 Spark jobs & worker status

🏭 ClickHouse UI http://localhost:8123 Query processed data

🐘 PostgreSQL Port 5433 Metadata database for Airflow

## ✈️ Workflow Summary

📡 Fetch flight data from the OpenSky API

📥 Load raw flight data into PostgreSQL for persistence

🕐 Wait for ClickHouse tables to be available

⚡ Trigger a Spark job inside the container for transformation

💾 Store processed data into ClickHouse for analytics

🧠 Log every stage for better observability

## ⚙️ Environment Variables

All required environment variables are defined in `.env.example.`

Simply copy and rename it before starting the containers:

`cp .env.example .env`

No manual exports required — Docker Compose automatically loads variables from .env.

## 🧹 Cleanup

Stop and remove everything (including volumes):

`docker-compose down -v`

## 🧠 Notes

🗂️ shared_data/ and spark/work/ are temporary folders (already in .gitignore)

⚙️ Health checks are disabled for better stability on low-memory systems

🧾 To view container logs:

`docker logs airflow-webserver | tail -n 50`

⭐ Enjoy exploring flight data pipelines powered by Airflow, Spark, and ClickHouse!
