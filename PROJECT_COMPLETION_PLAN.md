# Flight & Airport Big Data Analytics Platform
## 48-Hour Sprint: 4-Member Team Execution Plan & Architecture Defense (v2, patched)

> **Institution:** National Telecommunication Institute (NTI), Egypt  
> **Course:** Final Graduation Project, Big Data Training Program  
> **Team Size:** 4 Members  
> **Time Limit:** **Strict 2 Days (48 hours from kickoff)**  
> **Dataset Status:** **100% downloaded** (48 monthly CSV files, 12.50 GB, 2022–2025, ~27.4M records) in `data/raw/`  
> **Target Outcome:** Working Big Data pipeline + Power BI dashboard + final deck (PySpark MLlib model = optional extension).

### What changed in v2
- **Table contract:** 5 → **7 summary tables** + 1 ML table. Added `agg_calendar_delays` (Q5) and `agg_cancellation_reasons` (Q6). Fixed the Q8 mapping. Added the missing DDL for `agg_airport_performance`. All 8 tables now have DDL.
- **Metric definitions** added (Section 5.1), so every member computes identical numbers.
- **Parquet layer:** all jobs after the first read cleaned Parquet, not raw CSV (ADR 7).
- **ML fixed:** leak-free feature list, train/test split, `OneHotEncoder` (Spark 3), `maxBins` note, sampling (ADR 4, Section 6.5).
- **Spark → ClickHouse** via small CSV loads instead of JDBC (ADR 8).
- **Power BI ↔ ClickHouse connection tested in hours 0–3**, not hour 24. Import mode is the fallback.
- **Environment:** CentOS end-of-life, Python 3.6, and Airflow notes; Docker stack must include PostgreSQL; Sqoop is time-boxed.
- **Schedule** rewritten around these changes; **cut order** added (Section 9).

---

## 1. Executive Summary & the 48-Hour Fast-Track Strategy

A strict waterfall (infra → Spark → ML → BI) misses a 48-hour deadline. Instead the sprint is **contract-driven**:

1. **Hour 1:** All 4 members agree on the **7 summary tables + 1 ML table** (exact names, columns, types) and the **metric definitions** (Section 5). Changing the contract later costs four people's work.
2. **Hours 0–3:** Test the riskiest integration first: **Power BI → ClickHouse** (ODBC driver, connector, port) using a dummy table.
3. **Hours 2–24 (Day 1):** Everyone works in parallel on the same **1-month sample** (`2024_1.csv`, ~550k rows):
   - **Member 1** builds ingestion + Airflow around the contract paths.
   - **Member 2** writes the CSV→Parquet step and the 7 aggregations.
   - **Member 3** builds the leak-free ML pipeline on the sample.
   - **Member 4** builds the Power BI pages from sample CSV exports of the contract tables (Member 2 delivers them by hour 8 and hour 12).
4. **Hours 24–36 (Day 2):** Switch the input from the sample to the **full 48 months**. Convert CSV → Parquet **once**; every later run reads Parquet.
5. **Hours 36–48:** Airflow run, deck, README, rehearsal, submit.

> **Time reality:** 48 clock hours is not 48 working hours. Plan sleep in shifts. If behind, use the cut order in Section 9.

---

## 2. Architecture Decision Records (ADRs)

### ADR 1: Dedicated OLAP serving layer (ClickHouse) between Spark and Power BI
* **Context:** Power BI needs interactive filtering over aggregates of 27M records.
* **Alternative:** Power BI directly on Spark SQL / Hive.
* **Decision:** Spark computes rollups and writes them to ClickHouse `MergeTree` tables.
* **Rationale:** Spark / Hive are batch engines; a Thrift / ODBC query on every slicer click takes 10–30 s. ClickHouse is a vectorized columnar OLAP database with sub-second aggregations.
* **Power BI facts:** Power BI Desktop has a built-in ClickHouse connector that supports DirectQuery. It requires the **ClickHouse ODBC driver** installed first, and a recent Power BI Desktop version (the ClickHouse docs say the connector is on by default from version 2.137.751.0).
* **Fallback:** The aggregate tables are small. If DirectQuery fails, export tables to CSV and use **Import mode**; the dashboard works the same, only the "DirectQuery" claim goes away.

### ADR 2: Sqoop is scoped to relational reference data, and time-boxed
* **Context:** The architecture mandates Sqoop, but the primary dataset is 48 CSV files.
* **Decision:** Ingest raw CSVs with `hdfs dfs -put`. Use **Sqoop** to import the carrier / airport lookup tables from an RDBMS (PostgreSQL, or MySQL if it is already installed on the VM) into HDFS.
* **Rationale:** Sqoop is built for RDBMS ↔ Hadoop transfer; using it on flat files is an anti-pattern.
* **Risk:** Sqoop is a retired Apache project. Version 1.4.7 often fails on Hadoop 3 (missing JDBC driver / commons-lang jars in `$SQOOP_HOME/lib`, `HADOOP_MAPRED_HOME`).
* **Time-box:** **2 hours maximum.** If Sqoop is already working on the NTI VM from the course, use that setup.
* **Fallback:** Load the lookup CSV with `hdfs dfs -put`, keep the Sqoop command and PostgreSQL schema in the README as the designed path, and **say so honestly on Slide 5** (do not claim it ran).

### ADR 3: Dual environment (VM health check vs. Docker stack)
* **Context:** The existing CentOS VM has been modified and may be unstable.
* **Decision:** A 5-minute health check (Path A). If anything fails, switch to Docker Compose (Path B) within the first 2 hours.
* **Known VM problems to check in the health check:**
  * CentOS 7 / 8 are end-of-life; default `yum` repos may fail (they need `vault.centos.org`).
  * CentOS 7 system Python is 3.6. Recent Airflow 2.x needs Python ≥ 3.8, so use a Python ≥ 3.9 venv (do **not** use system Python).
  * Disk: need ≥ 40 GB free (raw 12.5 GB + Parquet + HDFS + images). RAM: ≥ 8 GB for VM / Docker.
  * Set `dfs.replication = 1` on a single node.
* **Docker stack requirements:** the Compose file must contain `namenode`, `datanode`, `spark-master`, `spark-worker`, `clickhouse`, **`postgres`** (for lookup tables). Airflow runs in a Python ≥ 3.9 venv or the official `apache/airflow` image in `standalone` mode. **Sqoop is VM-only** (no official Docker image).
* **Action (Member 1):** Confirm `docker-compose.yml` exists in the project root. If it does not, create it only if the VM health check fails. Do not copy 12.5 GB from the Windows drive through a bind mount (slow on WSL2); copy it into the WSL filesystem or into HDFS first.

### ADR 4: Distributed PySpark MLlib over Scikit-Learn (optional extension)
* **Context:** Predict flight delay risk before departure.
* **Decision:** MLlib (`pyspark.ml`) with `StringIndexer`, **`OneHotEncoder`** (in Spark 3.x; `OneHotEncoderEstimator` only exists in Spark 2.x), `VectorAssembler`, `LogisticRegression` (primary) and optionally `RandomForestClassifier`.
* **Rules:**
  * Features limited to what is known before departure (Section 5.2). Using departure delay, taxi times, arrival times, or delay causes is **data leakage**.
  * Train on a **10–20% sample of 2022–2023**; test on 2024.
  * If a tree model uses indexed Origin / Dest directly, set `maxBins ≥ 400` (about 350 airports); with one-hot features this is not needed.
  * Expect a modest AUC (about 0.60–0.70). That is normal; report it honestly.
* **Rationale:** Scikit-Learn needs all data in one machine's RAM; MLlib trains on Spark DataFrames.

### ADR 5: Two-phase development (sample first → full scale)
* **Decision:** Develop on one month (~550k rows, about 10 s per run). Switch the path to `data/raw/*.csv` only after the pipeline works end to end.

### ADR 6: Batch pipeline over real-time streaming
* **Decision:** 100% of effort goes to the batch pipeline. Kafka / Spark Structured Streaming is **not built**; it appears on the future-work slide (Slide 12). Kafka is not in the technology table for the same reason.

### ADR 7: Parquet intermediate layer (new)
* **Context:** Reading 12.5 GB of CSV on every run takes about 30 minutes.
* **Decision:** One job reads the 48 CSVs with an explicit column list (no `inferSchema`), keeps about 35 of 109 columns, casts types, derives `Dep_Hour`, and writes **Parquet partitioned by `Year`** to `/project/flights/parquet/`. Aggregations and ML read only this Parquet.
* **Result:** Expect about 1.5–2 GB of Parquet and runs of a few minutes instead of 30.

### ADR 8: Spark → ClickHouse through small CSV loads (new)
* **Context:** JDBC writes from Spark into ClickHouse often fail on type mismatches (ClickHouse `UInt32` vs. Spark signed types) and need extra jars.
* **Decision:** Aggregates are tiny. Use `toPandas().fillna(0).to_csv(...)` then `clickhouse-client --query "INSERT INTO ... FORMAT CSVWithNames" < file.csv`. Column **names** must match the DDL (order does not matter).
* **Exception:** `ml_delay_predictions` is loaded as a **≤ 500k-row sample**, not millions of rows.

---

## 3. 4-Member Work Breakdown

```
  [ Member 1: Infrastructure & Orchestration ]   [ Member 2: Spark Analytics & ETL ]
  • Owns: Environment, HDFS, PostgreSQL, Sqoop,  • Owns: CSV→Parquet, 7 aggregate tables,
          ClickHouse setup + DDL, Airflow                  CSV exports, ClickHouse loads
  • Deliverables:                                • Deliverables:
    - VM health check or Docker stack              - spark_csv_to_parquet.py
    - HDFS layout, ingestion, lookup tables        - spark_aggregates.py (Tables 1–7)
    - ClickHouse DB + 8 tables (DDL)               - Sample CSVs for Member 4 (hour 8 / 12)
    - flight_pipeline_dag.py (BashOperator tasks)  - Verification queries for Q1–Q8

  [ Member 3: ML Engineer ]                      [ Member 4: BI & Presentation Lead ]
  • Owns: MLlib pipeline (optional extension)    • Owns: Power BI + final deck + README
  • Deliverables:                                • Deliverables:
    - Leak-free delay classifier (ArrDel15)        - Power BI ↔ ClickHouse test (hours 0–3)
    - AUC-ROC, precision / recall, confusion       - 4-page Power BI dashboard
    - Feature importance / coefficients            - 12-slide deck + technical README
    - ml_delay_predictions (≤ 500k rows)
```

---

## 4. Hour-by-Hour Schedule

### Day 1: Foundation, parallel prototyping, integration (Hours 0–24)

| Window | Member 1 (Infra) | Member 2 (Spark) | Member 3 (ML) | Member 4 (BI / Deck) |
|---|---|---|---|---|
| **0–3** | **Health check** (daemons, disk ≥ 40 GB, RAM, Python version, `yum`). Decide VM vs. Docker by hour 2. **Start ClickHouse**, create `flight_analytics` and one dummy table; make port 8123 reachable from Windows. | Schema audit of `data/raw/`. **Confirm the 8-table contract + metric definitions with the team (hour 1).** Draft the explicit column list for CSV→Parquet. | Fix label (`ArrDel15`), allowed features, train / test split (Section 5.2). | Wireframe the 4 pages. **Install ClickHouse ODBC driver, update Power BI Desktop, connect to Member 1's dummy table (DirectQuery).** If it fails, use the Import fallback. |
| **3–8** | Load the 1-month sample to HDFS (`/project/flights/sample`). Create PostgreSQL (or MySQL) carrier lookup table. | Run CSV→Parquet on the sample. ETL core: casts, `Dep_Hour`, null handling. Produce **Tables 1 and 4** and send CSVs to Member 4 by **hour 8**. | Feature pipeline on the sample: `StringIndexer`, `OneHotEncoder`, `VectorAssembler` (allowed features only). | Build page layouts and KPI cards from the sample CSVs of Tables 1 and 4. |
| **8–14** | **Sqoop import (2-hour time-box)** PostgreSQL → HDFS; fall back per ADR 2 if needed. Then set up Airflow in a Python ≥ 3.9 venv. | Tables 2, 3, 5, 6, 7 on the sample. Send **all sample CSVs by hour 12**. | Baseline `LogisticRegression` (optional `RandomForest`) on a sample. Compute AUC. | Pages 1 and 2 (Executive Overview, Root Causes & Seasonality). |
| **14–20** | Create all **8 ClickHouse tables** from the DDL (Section 6.3). Start the Airflow DAG skeleton. | Load the sample aggregates into ClickHouse via CSV (ADR 8). | Coefficients / feature importance. Build predictions sample (≤ 500k rows). | Pages 3 and 4 (Route Corridors, ML Risk). |
| **20–24** | Airflow draft: `check_data → csv_to_parquet → spark_aggregates → load_clickhouse → train_model` as `BashOperator`s. | Validate all ClickHouse tables against raw numbers. | Load predictions into `ml_delay_predictions`. | End-to-end test with the real sample tables in ClickHouse (connection already proven in hours 0–3). |

### Day 2: Full-scale run, polish, defense readiness (Hours 24–48)

| Window | Member 1 | Member 2 | Member 3 | Member 4 |
|---|---|---|---|---|
| **24–30** | Bulk-load the 48 CSVs to `/project/flights/raw/`. | **Run CSV→Parquet once on all 48 months.** Then run aggregates from Parquet. | Train on 10–20% sample of 2022–2023, evaluate on 2024; record AUC-ROC and metrics. | Slides 1–6. |
| **30–36** | Log run times and storage sizes. | Bulk-load final aggregates into ClickHouse; verify row counts. | Document confusion matrix, precision / recall, top drivers. | Refresh Power BI on full tables; check every visual. |
| **36–42** | Finalize and trigger the Airflow DAG; screenshot the green tree view. (If the full run is too long, trigger it on the Parquet-based stages.) | Run verification queries for Q1–Q8 for the report. | ROC curve and feature-importance visuals for the deck. | Slides 7–12 with screenshots. |
| **42–48** | Assemble master `README.md`. | Code cleanup and docstrings. | Prepare a short live demo / terminal script for Q&A. | Two full rehearsals (10–12 min). **SUBMIT.** |

---

## 5. Contract: 7 Summary Tables + 1 ML Table

### 5.1 Metric definitions (everyone uses exactly these)
- **Total_Flights** = all rows, including cancelled and diverted.
- **Operated flight** = `Cancelled = 0` AND `Diverted = 0` AND `ArrDel15` not null.
- **Delayed_Flights** = operated with `ArrDel15 = 1`; **OnTime_Flights** = operated with `ArrDel15 = 0`.
- **Delay_Rate_Pct** = Delayed / (Delayed + OnTime) × 100. **Cancellation_Rate_Pct** = Cancelled / Total_Flights × 100.
- **Dep_Delay_Rate_Pct / Arr_Delay_Rate_Pct** = share with `DepDel15 = 1` / `ArrDel15 = 1`.
- **Avg_*_Delay_Min** = average of `DepDelayMinutes` / `ArrDelayMinutes` (early = 0) over operated flights.
- **Total_Delay_Minutes** = sum of the five cause columns (null → 0). **Cause_Pct** = cause minutes / Total_Delay_Minutes × 100.
- **Dep_Hour** = `floor(CRSDepTime / 100) % 24`. **Day_Of_Week**: 1 = Monday … 7 = Sunday (BTS `DayOfWeek`).
- **Season** (Power BI): Dec–Feb Winter, Mar–May Spring, Jun–Aug Summer, Sep–Nov Fall.
- **Route** is directed (`Origin-Dest`). **Cancellation_Reason**: A = Carrier, B = Weather, C = NAS, D = Security.

### 5.2 ML rules
- **Allowed features:** `Month`, `DayOfWeek`, `Dep_Hour`, `Reporting_Airline`, `Origin`, `Dest`, `Distance`, `CRSElapsedTime`.
- **Forbidden (leakage):** `DepTime`, `DepDelay*`, `DepDel15`, `ArrTime`, `ActualElapsedTime`, `AirTime`, `ArrDelay*`, all cause columns, `Cancelled`, `Diverted`, taxi / wheels fields.
- **Population:** operated flights only. **Split:** train 2022–2023 (10–20% sample), test 2024.

### 5.3 Tables

**Table 1: `agg_airline_performance`** — Q1, Q6 (rates), Q8 (sum `Total_Flights` by `Year`)  
`Year` UInt16, `Reporting_Airline` String, `Airline_Name` String, `Total_Flights` UInt32, `OnTime_Flights` UInt32, `Delayed_Flights` UInt32, `Delay_Rate_Pct` Float32, `Avg_Dep_Delay_Min` Float32, `Avg_Arr_Delay_Min` Float32, `Cancelled_Flights` UInt32, `Cancellation_Rate_Pct` Float32

**Table 2: `agg_airport_performance`** — Q2, Q6 (rates)  
`Airport_Code` String, `Airport_City` String, `Airport_State` String, `Total_Departures` UInt32, `Total_Arrivals` UInt32, `Dep_Delay_Rate_Pct` Float32, `Arr_Delay_Rate_Pct` Float32, `Avg_Dep_Delay_Min` Float32, `Cancelled_Departures` UInt32

**Table 3: `agg_delay_causes_monthly`** — Q4  
`Year` UInt16, `Month` UInt8, `Total_Delay_Minutes` UInt64, `Carrier_Delay_Min` / `Weather_Delay_Min` / `NAS_Delay_Min` / `Security_Delay_Min` / `Late_Aircraft_Delay_Min` UInt64, `Carrier_Pct` / `Weather_Pct` / `NAS_Pct` / `Security_Pct` / `Late_Aircraft_Pct` Float32

**Table 4: `agg_hourly_delays`** — Q3  
`Dep_Hour` UInt8 (0–23), `Total_Flights` UInt32, `Delayed_Flights` UInt32, `Delay_Probability_Pct` Float32, `Avg_Delay_Minutes` Float32

**Table 5: `agg_route_traffic`** — Q7  
`Origin` String, `Dest` String, `Route_Name` String, `Flight_Count` UInt32, `Avg_AirTime_Min` Float32, `Distance_Miles` UInt32, `Route_Delay_Rate_Pct` Float32

**Table 6 (new): `agg_calendar_delays`** — Q5, Q8  
`Year` UInt16, `Month` UInt8, `Day_Of_Week` UInt8, `Total_Flights` UInt32, `Delayed_Flights` UInt32, `Cancelled_Flights` UInt32, `Delay_Rate_Pct` Float32, `Avg_Arr_Delay_Min` Float32

**Table 7 (new): `agg_cancellation_reasons`** — Q6 (reasons)  
`Year` UInt16, `Reporting_Airline` String, `Cancellation_Code` String, `Cancellation_Reason` String, `Cancelled_Flights` UInt32

**Table 8: `ml_delay_predictions`** — Power BI Page 4 (≤ 500k-row sample of the 2024 test set)  
`FlightDate` Date, `Reporting_Airline` String, `Origin` String, `Dest` String, `Dep_Hour` UInt8, `Actual_Delayed` UInt8, `Predicted_Delayed` UInt8, `Delay_Probability` Float32, `Risk_Category` String (Low / Medium / High)

### 5.4 Question coverage check

| Q | Table(s) |
|---|---|
| Q1 | 1 |
| Q2 | 2 |
| Q3 | 4 |
| Q4 | 3 |
| Q5 | 6 (+ Season in Power BI) |
| Q6 | 1, 2 (rates) + 7 (reasons) |
| Q7 | 5 |
| Q8 | 1 or 6 |

---

## 6. Fail-Safe Execution Commands

> The code in 6.4–6.5 is a **starter, not tested against your cluster**. Adjust paths and cluster options. Run it first on the 1-month sample.

### 6.1 Path A: VM health check
```bash
# 1. Resources and Python (CentOS end-of-life / old Python checks)
df -h ; free -g ; python3 --version ; java -version

# 2. Start core Hadoop daemons
start-dfs.sh && start-yarn.sh
jps          # MUST show: NameNode, DataNode, ResourceManager, NodeManager

# 3. HDFS write test
hdfs dfs -mkdir -p /project/flights/raw /project/flights/parquet /project/reference
hdfs dfs -ls /project/flights

# 4. Spark readiness
spark-shell --version

# 5. Can we install anything? (fails on EOL CentOS repos unless vault is configured)
yum --version && yum makecache 2>&1 | tail -3
```

### 6.2 Path B: Docker (if the VM fails)
```powershell
# Needs namenode, datanode, spark-master, spark-worker, clickhouse, postgres
docker compose up -d
docker ps
```

### 6.3 ClickHouse DDL (all 8 tables)
```sql
CREATE DATABASE IF NOT EXISTS flight_analytics;
USE flight_analytics;

CREATE TABLE IF NOT EXISTS agg_airline_performance (
    Year UInt16, Reporting_Airline String, Airline_Name String,
    Total_Flights UInt32, OnTime_Flights UInt32, Delayed_Flights UInt32,
    Delay_Rate_Pct Float32, Avg_Dep_Delay_Min Float32, Avg_Arr_Delay_Min Float32,
    Cancelled_Flights UInt32, Cancellation_Rate_Pct Float32
) ENGINE = MergeTree() ORDER BY (Year, Reporting_Airline);

CREATE TABLE IF NOT EXISTS agg_airport_performance (
    Airport_Code String, Airport_City String, Airport_State String,
    Total_Departures UInt32, Total_Arrivals UInt32,
    Dep_Delay_Rate_Pct Float32, Arr_Delay_Rate_Pct Float32,
    Avg_Dep_Delay_Min Float32, Cancelled_Departures UInt32
) ENGINE = MergeTree() ORDER BY Airport_Code;

CREATE TABLE IF NOT EXISTS agg_delay_causes_monthly (
    Year UInt16, Month UInt8, Total_Delay_Minutes UInt64,
    Carrier_Delay_Min UInt64, Weather_Delay_Min UInt64, NAS_Delay_Min UInt64,
    Security_Delay_Min UInt64, Late_Aircraft_Delay_Min UInt64,
    Carrier_Pct Float32, Weather_Pct Float32, NAS_Pct Float32,
    Security_Pct Float32, Late_Aircraft_Pct Float32
) ENGINE = MergeTree() ORDER BY (Year, Month);

CREATE TABLE IF NOT EXISTS agg_hourly_delays (
    Dep_Hour UInt8, Total_Flights UInt32, Delayed_Flights UInt32,
    Delay_Probability_Pct Float32, Avg_Delay_Minutes Float32
) ENGINE = MergeTree() ORDER BY Dep_Hour;

CREATE TABLE IF NOT EXISTS agg_route_traffic (
    Origin String, Dest String, Route_Name String, Flight_Count UInt32,
    Avg_AirTime_Min Float32, Distance_Miles UInt32, Route_Delay_Rate_Pct Float32
) ENGINE = MergeTree() ORDER BY (Origin, Dest);

CREATE TABLE IF NOT EXISTS agg_calendar_delays (
    Year UInt16, Month UInt8, Day_Of_Week UInt8,
    Total_Flights UInt32, Delayed_Flights UInt32, Cancelled_Flights UInt32,
    Delay_Rate_Pct Float32, Avg_Arr_Delay_Min Float32
) ENGINE = MergeTree() ORDER BY (Year, Month, Day_Of_Week);

CREATE TABLE IF NOT EXISTS agg_cancellation_reasons (
    Year UInt16, Reporting_Airline String, Cancellation_Code String,
    Cancellation_Reason String, Cancelled_Flights UInt32
) ENGINE = MergeTree() ORDER BY (Year, Reporting_Airline, Cancellation_Code);

CREATE TABLE IF NOT EXISTS ml_delay_predictions (
    FlightDate Date, Reporting_Airline String, Origin String, Dest String,
    Dep_Hour UInt8, Actual_Delayed UInt8, Predicted_Delayed UInt8,
    Delay_Probability Float32, Risk_Category String
) ENGINE = MergeTree() ORDER BY (FlightDate, Reporting_Airline);

-- Hours 0-3 connectivity test table (drop later)
CREATE TABLE IF NOT EXISTS connectivity_test (id UInt8, note String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO connectivity_test VALUES (1, 'power bi test');
```

### 6.4 CSV → Parquet (run once; starter)
```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("flights_csv_to_parquet").getOrCreate()

RAW = "hdfs:///project/flights/raw/*.csv"          # sample: .../sample/2024_1.csv
OUT = "hdfs:///project/flights/parquet"

COLS = {  # column -> type (selected by NAME; BTS rows end with a trailing comma)
    "Year": "int", "Month": "int", "DayofMonth": "int", "DayOfWeek": "int", "FlightDate": "date",
    "Reporting_Airline": "string", "Tail_Number": "string",
    "Flight_Number_Reporting_Airline": "string",
    "Origin": "string", "OriginCityName": "string", "OriginState": "string",
    "Dest": "string", "DestCityName": "string", "DestState": "string",
    "CRSDepTime": "double", "DepTime": "double", "DepDelay": "double",
    "DepDelayMinutes": "double", "DepDel15": "double",
    "CRSArrTime": "double", "ArrTime": "double", "ArrDelay": "double",
    "ArrDelayMinutes": "double", "ArrDel15": "double",
    "Cancelled": "double", "CancellationCode": "string", "Diverted": "double",
    "CRSElapsedTime": "double", "ActualElapsedTime": "double", "AirTime": "double",
    "Distance": "double", "CarrierDelay": "double", "WeatherDelay": "double",
    "NASDelay": "double", "SecurityDelay": "double", "LateAircraftDelay": "double",
}

df = spark.read.option("header", True).csv(RAW)          # all strings: no inferSchema pass
df = df.select([F.col(c).cast(t).alias(c) for c, t in COLS.items()])
df = (df.withColumn("Dep_Hour", (F.floor(F.col("CRSDepTime") / 100) % 24).cast("int"))
        .withColumn("Route", F.concat_ws("-", "Origin", "Dest")))

df.write.mode("overwrite").partitionBy("Year").parquet(OUT)
```

### 6.5 Load aggregates into ClickHouse, and ML starter

**Aggregate → CSV → ClickHouse (ADR 8):**
```python
import os
os.makedirs("/tmp/out", exist_ok=True)
t1.toPandas().fillna(0).to_csv("/tmp/out/agg_airline_performance.csv", index=False)   # repeat per table
```
```bash
clickhouse-client --query "INSERT INTO flight_analytics.agg_airline_performance FORMAT CSVWithNames" \
  < /tmp/out/agg_airline_performance.csv
# Docker:  docker exec -i clickhouse clickhouse-client --query "..." < file.csv
```

**ML starter (leak-free):**
```python
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

df = spark.read.parquet("hdfs:///project/flights/parquet")
df = df.filter((F.col("Cancelled") == 0) & (F.col("Diverted") == 0) & F.col("ArrDel15").isNotNull())
df = df.withColumn("label", F.col("ArrDel15"))

cat = ["Reporting_Airline", "Origin", "Dest"]
num = ["Month", "DayOfWeek", "Dep_Hour", "Distance", "CRSElapsedTime"]     # allowed features only

stages  = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in cat]
stages += [OneHotEncoder(inputCols=[c + "_idx" for c in cat],
                         outputCols=[c + "_ohe" for c in cat], handleInvalid="keep")]
stages += [VectorAssembler(inputCols=[c + "_ohe" for c in cat] + num,
                           outputCol="features", handleInvalid="skip")]
stages += [LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)]

train = df.filter(F.col("Year") <= 2023).sample(fraction=0.15, seed=42)
test  = df.filter(F.col("Year") == 2024)                         # sample first if slow

model = Pipeline(stages=stages).fit(train)
pred  = model.transform(test)

auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred)
print("AUC-ROC:", auc)                         # ~0.60-0.70 is expected; much higher = leakage

out = (pred.withColumn("p", vector_to_array("probability")[1])
           .select("FlightDate", "Reporting_Airline", "Origin", "Dest", "Dep_Hour",
                   F.col("label").cast("int").alias("Actual_Delayed"),
                   F.col("prediction").cast("int").alias("Predicted_Delayed"),
                   F.col("p").cast("float").alias("Delay_Probability"))
           .withColumn("Risk_Category",
                       F.when(F.col("Delay_Probability") < 0.15, "Low")
                        .when(F.col("Delay_Probability") < 0.30, "Medium").otherwise("High"))
           .sample(fraction=0.07, seed=1).limit(500000))          # <= 500k rows for ClickHouse
```

---

## 7. Final Presentation Structure (12 slides)

* **Slide 1:** Title, team, NTI Big Data specialization. *(Member 4)*
* **Slide 2:** Business problem & motivation. *(Member 4)*
* **Slide 3:** Dataset (BTS 2022–2025, 27.4M flights, 109 attributes, 12.5 GB). *(Member 1)*
* **Slide 4:** End-to-end architecture (CSV → HDFS → Parquet → Spark → ClickHouse → Power BI, Airflow on top). *(Member 1)*
* **Slide 5:** Storage & relational ingestion (HDFS layout + Sqoop import; **state honestly if Sqoop fell back**). *(Member 1)*
* **Slide 6:** Spark processing & cleaning rules (null handling, `Dep_Hour`, metric definitions). *(Member 2)*
* **Slide 7:** Core findings (Q1 worst airlines, Q2 congested airports, Q7 busiest routes). *(Member 2)*
* **Slide 8:** Temporal & root cause analysis (Q3, Q4, Q5, plus Q6 cancellations). *(Member 2)*
* **Slide 9:** ML formulation (binary classification, leak-free features, train 2022–23 / test 2024). *(Member 3)*
* **Slide 10:** Model evaluation & feature importance (AUC-ROC, honest interpretation). *(Member 3)*
* **Slide 11:** Power BI dashboard showcase (4 pages). *(Member 4)*
* **Slide 12:** Conclusions, Airflow orchestration, **future work: real-time streaming with Kafka + Spark Structured Streaming**. *(Member 4)*

---

## 8. Deliverables Checklist

- [x] **Raw dataset:** 48 monthly CSVs (12.5 GB) in `data/raw/`.
- [ ] **Contract agreed (hour 1):** 8 tables + metric definitions.
- [ ] **Power BI ↔ ClickHouse connection proven (by hour 3).**
- [ ] **Environment validated:** CentOS VM or Docker stack up (disk / RAM / Python checked).
- [ ] **Storage:** HDFS raw + Parquet populated; lookup table imported (Sqoop, or documented fallback).
- [ ] **Processing:** `spark_csv_to_parquet.py` and `spark_aggregates.py` generate Tables 1–7.
- [ ] **Serving:** All 8 ClickHouse tables populated; Q1–Q8 verified with SQL.
- [ ] **ML (optional):** leak-free model, AUC-ROC reported, `ml_delay_predictions` loaded (≤ 500k rows).
- [ ] **BI:** 4-page `.pbix` with slicers.
- [ ] **Orchestration:** `flight_pipeline_dag.py` visible and green in Airflow.
- [ ] **Documentation:** `README.md` and 12-slide deck.

---

## 9. If You Fall Behind: Cut Order

1. **ML model and Power BI page 4** (not in the original project objectives).
2. **Airflow polish:** keep a simple DAG of `BashOperator`s with one green run.
3. **Extra visuals** beyond the 8 questions.
4. **Never cut:** CSV→Parquet, the 7 aggregate tables, ClickHouse, and Power BI pages 1–3.
