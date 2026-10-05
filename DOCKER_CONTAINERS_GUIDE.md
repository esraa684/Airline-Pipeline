# Docker & Big Data Containers Live Demonstration Guide
### Comprehensive Guide & Step-by-Step Instructor Presentation Script

---

> **Guide Objective:**  
> Explain the concepts of **Containers and Docker from absolute scratch**, detail every single service running in our cluster, and provide a **complete, step-by-step Live Demonstration Script** to present your environment in front of your instructor with 100% technical confidence.

---

## Table of Contents
1. [What is a Container? (The Shipping Container Analogy)](#1-what-is-a-container-the-shipping-container-analogy)
2. [What is Docker & Docker Compose?](#2-what-is-docker--docker-compose)
3. [Our Cluster Architecture: The Running Services](#3-our-cluster-architecture-the-running-services)
4. [Step-by-Step Live Instructor Demo Script](#4-step-by-step-live-instructor-demo-script)
   - [Step 1: Check Docker Desktop](#step-1-ensure-docker-desktop-is-running)
   - [Step 2: Spin Up the Cluster](#step-2-spin-up-the-cluster)
   - [Step 3: Verify Running Containers in CLI](#step-3-verify-running-containers-in-cli)
   - [Step 4: The 5-Tab Browser Showcase](#step-4-the-5-tab-browser-showcase)
   - [Step 5: Live Container Execution](#step-5-live-container-execution)
5. [Common Instructor Questions & Exact Winning Answers](#5-common-instructor-questions--exact-winning-answers)
6. [Emergency Troubleshooting Cheat Sheet](#6-emergency-troubleshooting-cheat-sheet)

---

## 1. What is a Container? (The Shipping Container Analogy)

### The Old Way: Heavy Virtual Machines (VMs)
Before containers, running a Big Data stack required installing a full operating system (like Ubuntu or CentOS) inside a Virtual Machine software (like VirtualBox or VMware):
- A Virtual Machine takes **20 to 40 GB** of disk space per OS.
- It reserves **4 to 8 GB of RAM** just to run the guest OS background daemons.
- It takes **3 to 5 minutes** to boot up.
- Most frustratingly, you hit the classic developer headache: *"It works on my machine, why doesn't it work on yours?"* because of mismatched Java, Python, or operating system package versions.

### The Modern Solution: Docker Containers
Think of modern **ocean freight cargo ships**:
Goods (cars, electronics, frozen food) are loaded into standardized, sealed **steel shipping containers**. The cargo crane doesn't care what is inside; it moves the container seamlessly from ship to truck to train.

**A Docker container does the exact same thing for software:**
- It packages the program (e.g., Apache Spark or ClickHouse) together with all of its libraries, environment variables, and dependencies into an isolated, lightweight box.
- **The key difference vs. a VM:** Containers do **not** run a separate guest OS kernel. They share the host machine's Linux kernel (via WSL2 on Windows).
- **Result:** Containers boot up in **under 2 seconds**, use only the RAM needed by the application, and guarantee that what runs on your laptop runs identically on any server in the world.

---

## 2. What is Docker & Docker Compose?

- **Docker Engine:** The background runtime engine that builds, runs, and isolates containers on your operating system.
- **Docker Image:** The frozen blueprint or recipe (e.g., a pre-built image containing Ubuntu + Apache Spark 3.3.0 + Java 11).
- **Docker Container:** The live, running instance of an image consuming CPU and RAM.
- **Docker Compose:**  
  Our Big Data platform requires 8 different specialized tools running simultaneously. Typing 8 lengthy `docker run` commands with dozens of port mappings and network flags in a terminal is error-prone.  
  With **Docker Compose**, we declare our entire multi-node cluster in a single readable configuration file: `docker-compose.yml`.  
  With one single command:
  ```powershell
  docker compose up -d
  ```
  Docker automatically pulls the images, connects them to a private virtual network (`bigdata-net`), sets up storage volumes, and launches the entire cluster in seconds.

---

## 3. Our Cluster Architecture: The Running Services

All containers communicate over an isolated Docker network called `bigdata-net`. Within this network, containers discover each other automatically by name (internal Docker DNS) without hardcoded IP addresses.

| Container Name | Technology | Host Ports | Role in our Big Data Platform |
| :--- | :--- | :--- | :--- |
| **`namenode`** | **Hadoop HDFS Master** | `9000` (RPC)<br>`9870` (Web UI) | The master coordinator of distributed storage; manages the filesystem namespace and block locations. |
| **`datanode`** | **Hadoop HDFS Worker** | `9864` (Web UI) | The physical storage worker; holds raw CSV and Parquet data blocks on disk. |
| **`spark-master`** | **Apache Spark 3.3.0 Master** | `7077` (Cluster RPC)<br>`8080` (Web UI) | The cluster orchestrator for distributed in-memory compute; schedules ETL and ML stages. |
| **`spark-worker`** | **Apache Spark Worker** | `8081` (Web UI) | The computational worker node executing parallel tasks (configured with 2 CPU Cores & 3 GB RAM). |
| **`clickhouse`** | **ClickHouse Server** | `8123` (HTTP)<br>`9009` (Native TCP) | High-speed columnar OLAP database serving sub-second analytical queries (~7–15 ms) for BI dashboards. |
| **`airflow-postgres`** | **PostgreSQL 13** | `5432` | Relational database dedicated to storing Apache Airflow metadata, DAG states, and task run histories. |
| **`kafka`** | **Apache Kafka (KRaft mode)** | `9092` (Internal)<br>`9094` (External) | Next-generation event streaming broker; handles real-time flight ingestion without ZooKeeper. |
| **`kafka-ui`** | **Kafka Web Console** | `8090` (Web UI) | Graphical browser interface to monitor Kafka brokers, topics, partitions, and real-time message streams. |
| **`minio`** *(Optional/Modern)* | **MinIO Object Storage** | `9000` (S3 API)<br>`9001` (Web Console) | High-performance, S3-compatible cloud-native object storage for modern Data Lakehouse architectures. |

---

## 4. Step-by-Step Live Instructor Demo Script

Follow this exact walkthrough when demonstrating the cluster to your instructor.

### Step 1: Ensure Docker Desktop is Running
1. Verify the Docker Desktop icon in your Windows taskbar is active (green status).

### Step 2: Spin Up the Cluster
Open Windows PowerShell and navigate to the project's `docker` folder:
```powershell
cd C:\Users\Abdelwadoud\Documents\BigData-NTI-Files\BigDataFinalProject\docker
docker compose up -d
```
*(If already running, this will complete in 1 to 2 seconds).*

### Step 3: Verify Running Containers in CLI
Run this command to display a clean, professional status table:
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

**What to SAY to the instructor:**
> *"Dr., here is our complete multi-container Big Data cluster running on an isolated virtual network called `bigdata-net`. We have separated storage and compute: Hadoop HDFS handles distributed storage, Apache Spark 3.3.0 provides distributed in-memory processing, ClickHouse serves sub-second OLAP queries, PostgreSQL manages Airflow metadata, and Apache Kafka handles real-time event streaming."*

---

### Step 4: The 5-Tab Browser Showcase

Open Google Chrome with the following tabs prepared:

#### 1. Hadoop HDFS Web UI
- **URL:** [http://localhost:9870](http://localhost:9870)
- **What to click:** In the top navigation bar, click **Utilities** $\rightarrow$ **Browse the file system**.
- **Navigate to:** `/project/flights/`
- **What to show:**
  - `/project/flights/raw/` (Raw CSV data).
  - `/project/flights/parquet/` (Optimized Parquet data partitioned by `Year=2024`).
- **What to SAY:**
  > *"Here is the Hadoop Distributed File System. We ingested raw BTS flight CSVs and converted them using PySpark into partitioned Parquet format. By using Snappy-compressed columnar Parquet, we reduced the physical storage footprint by over 90% while achieving 100x faster analytical read speeds."*

#### 2. Apache Spark Master Web UI
- **URL:** [http://localhost:8080](http://localhost:8080)
- **What to show:**
  - **Workers section:** Shows active Spark workers, available CPU cores, and memory allocation.
  - **Completed Applications:** Shows our executed jobs: `flights_csv_to_parquet` and `FlightAnalyticsAggregates`.
- **What to SAY:**
  > *"This is the Spark Master cluster manager. It coordinates distributed in-memory transformations across our worker nodes. Our PySpark aggregation pipeline processed 550,000+ flight records and computed all 7 business summary tables across multiple dimensions in just 51 seconds."*

#### 3. ClickHouse Analytical Serving Layer
- **URL:** [http://localhost:8123/play](http://localhost:8123/play)
- **What to show:** In the query editor, type and run:
  ```sql
  SHOW TABLES FROM flight_analytics;
  ```
  Then run:
  ```sql
  SELECT count() FROM flight_analytics.agg_route_traffic;
  ```
- **What to SAY:**
  > *"This is our ClickHouse OLAP serving database. Instead of having Power BI query Spark directly—which would introduce 20-30 second dashboard lags—Spark loads the pre-aggregated metrics into ClickHouse. As you can see, ClickHouse answers queries across thousands of route corridors in just 7 milliseconds."*

#### 4. Apache Kafka Web Console
- **URL:** [http://localhost:8090](http://localhost:8090)
- **What to show:** The Kafka-UI dashboard showing the cluster status as **Online** with zero active controller errors.
- **What to SAY:**
  > *"Here is our event streaming layer: Apache Kafka running in KRaft mode without ZooKeeper. We have a live flight event producer that publishes real-time departure and arrival records into Kafka topics for streaming analytics."*

---

### Step 5: Live Container Execution

Demonstrate terminal proficiency by running queries directly inside the containers:

```powershell
# 1. Query HDFS directly from the NameNode container:
docker exec -it namenode hdfs dfs -ls /project/flights/

# 2. Query ClickHouse row counts live:
docker exec -it clickhouse clickhouse-client --user default --password clickhouse --query "SELECT count() FROM flight_analytics.agg_airport_performance"
```

---

## 5. Common Instructor Questions & Exact Winning Answers

### Q1: "Why use Docker instead of running directly on Windows or a VirtualBox VM?"
**Winning Answer:**  
*"A production Big Data pipeline requires multiple specialized distributed services. Running everything on Windows natively leads to severe dependency collisions and port conflicts. A monolithic Virtual Machine consumes 30+ GB of disk and heavy memory overhead. Docker gives us isolated, lightweight, reproducible microservices sharing the host kernel, enabling our entire 4-person team to work in an identical environment."*

### Q2: "How does Spark communicate with HDFS inside Docker?"
**Winning Answer:**  
*"All containers are attached to the same custom bridge network (`bigdata-net`). Docker provides automatic internal DNS resolution, so Spark connects to HDFS using the standard URI `hdfs://namenode:9000/` without needing to hardcode dynamic container IP addresses."*

### Q3: "If you stop or restart your containers, will all your data be lost?"
**Winning Answer:**  
*"No. All stateful services (`namenode`, `datanode`, `clickhouse`, `postgres`) use Docker persistent volume mounts mapped to local host storage. Even if the containers are destroyed and recreated, all HDFS blocks and ClickHouse tables remain 100% intact."*

### Q4: "Why did you remap ClickHouse to port 9009?"
**Winning Answer:**  
*"The default ClickHouse native TCP port is `9000`, which conflicts with the standard Hadoop HDFS NameNode RPC port (`9000`) on the host. To eliminate this collision, we remapped the external ClickHouse native port to `9009:9000` while keeping the HTTP analytical port on `8123`."*

### Q5: "How does this scale to a real production cluster?"
**Winning Answer:**  
*"In our Docker Compose environment, we can scale worker nodes horizontally with a single flag: `docker compose up --scale spark-worker=2 -d`. In an enterprise cloud production setup (AWS or GCP), this same container architecture transitions directly to Kubernetes (EKS/GKE) using Helm charts or cloud-managed S3/Databricks infrastructure."*

---

## 6. Emergency Troubleshooting Cheat Sheet

| Situation | PowerShell Command to Fix |
| :--- | :--- |
| **Check running containers** | `docker ps` |
| **Start the full cluster** | `docker compose up -d` |
| **Stop the full cluster cleanly** | `docker compose stop` |
| **Restart a specific container** | `docker restart clickhouse` or `docker restart spark-master` |
| **View real-time container logs** | `docker logs clickhouse --tail 50 -f` |
| **Scale Spark to 2 workers** | `docker compose up --scale spark-worker=2 -d` |
