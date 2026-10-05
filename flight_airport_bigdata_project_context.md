# Flight & Airport Big Data Analytics Platform — Project Context (v2, patched)

> **Purpose of this file:** Full project context for AI agents and reviewers. Treat everything below as the source of truth for scope, architecture, dataset, table contracts, metric definitions, and ML rules.
>
> **v2 changes vs v1:** added a Question → Table mapping; added 2 missing summary tables (7 total + 1 ML table); added metric definitions; added a Parquet intermediate layer; added a leak-free ML specification; fixed Spark 3 API notes; added risks/fallbacks; clarified that Streaming/Kafka is **not built** (roadmap only).

---

## 1. Project Metadata

| Field | Value |
|---|---|
| **Project Name** | Flight & Airport Big Data Analytics Platform |
| **Type** | Final Graduation Project, Big Data Training Program |
| **Institution** | National Telecommunication Institute (NTI), Egypt |
| **Target Audience** | Airlines, Airport Authorities, Civil Aviation Planners, NTI Evaluation Committee |
| **Team Size** | 4 Members |
| **Delivery Deadline** | **2 Days (48 hours from kickoff)** |
| **Core Objective** | A scalable Big Data solution for analyzing flight operations, delay root causes, cancellations, and route performance using ~27.4M real-world aviation records. |

---

## 2. Problem Statement & Motivation

- **High financial & operational impact:** Flight delays and cancellations cost airlines and passengers billions of dollars annually (fuel burn, crew rescheduling, maintenance, lost productivity).
- **Scale bottlenecks:** Traditional RDBMS struggle to clean and aggregate multi-year aviation logs of tens of millions of rows within acceptable latency.
- **Need for scalable architecture:** A unified Big Data platform is needed to ingest bulk flight records, store them in a distributed file system, transform them with distributed computing, serve aggregates at sub-second speed, and surface insights interactively.

---

## 3. Project Objectives

1. **Operational benchmarking:** Rank airline and airport on-time performance.
2. **Root cause analysis:** Isolate drivers of delay: Carrier, Weather, NAS, Security, Late Aircraft.
3. **Temporal & seasonal patterns:** Delay distribution by hour, day of week, month/season, 2022–2025.
4. **Cancellation dynamics:** Cancellation rates by carrier and airport, and by cancellation code.
5. **Route intelligence:** Traffic volume, distance, air time, and delay rate on major corridors.
6. **Executive decision support:** Interactive 4-page Power BI dashboard.
7. **Predictive intelligence (ML extension — first thing to cut if behind schedule):** Distributed classifier predicting delay risk before departure.

---

## 4. Key Questions → Tables (strict rubric mapping)

| # | Question | Answered by |
|---|---|---|
| **Q1** | Which airlines have the highest delay rates and average delay duration? | `agg_airline_performance` |
| **Q2** | Which origin/destination airports experience the most delays? | `agg_airport_performance` |
| **Q3** | What are the peak hours for flight delays? | `agg_hourly_delays` |
| **Q4** | What is the dominant root cause of delays? | `agg_delay_causes_monthly` |
| **Q5** | How do delay probabilities change by month, season, and day of week? | `agg_calendar_delays` (season derived from Month in Power BI) |
| **Q6** | What is the cancellation rate per airline/airport, and the main reasons? | Rates: `agg_airline_performance`, `agg_airport_performance`. Reasons: `agg_cancellation_reasons` |
| **Q7** | What are the highest-traffic routes? | `agg_route_traffic` |
| **Q8** | How has domestic traffic volume evolved year over year (2022–2025)? | `agg_airline_performance` (sum `Total_Flights` by `Year`) or `agg_calendar_delays` |

---

## 5. Dataset Specification

### 5.1 Source
U.S. Bureau of Transportation Statistics (BTS), **Reporting Carrier On-Time Performance (1987–Present)**.

| Property | Value |
|---|---|
| **Time range used** | 2022 – 2025 |
| **Files** | 48 monthly CSV files (12 months × 4 years), in `data/raw/` |
| **Uncompressed size** | ~12.5 GB |
| **Records** | ~27.4 million flights (~550k–650k per month) |
| **Fields per record** | 109 |
| **Dev sample** | One month, `2024_1.csv` (~550k rows) — all members develop against it first |

> The original infographic says "47 files / ~11.4 GB". That is outdated. Use the numbers in this table.

### 5.2 Columns kept after cleaning (~35 of 109)
- **Calendar:** `Year`, `Month`, `DayofMonth`, `DayOfWeek` (1 = Monday … 7 = Sunday), `FlightDate`
- **Carrier / flight:** `Reporting_Airline`, `Tail_Number`, `Flight_Number_Reporting_Airline`
- **Routing:** `Origin`, `OriginCityName`, `OriginState`, `Dest`, `DestCityName`, `DestState`, `Distance`
- **Schedule / times:** `CRSDepTime`, `DepTime`, `CRSArrTime`, `ArrTime`, `CRSElapsedTime`, `ActualElapsedTime`, `AirTime`
- **Delays (minutes):** `DepDelay`, `DepDelayMinutes`, `DepDel15`, `ArrDelay`, `ArrDelayMinutes`, `ArrDel15`
- **Delay causes (minutes):** `CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay`
- **Cancellation / diversion:** `Cancelled`, `CancellationCode` (A = Carrier, B = Weather, C = NAS, D = Security), `Diverted`
- **Derived:** `Dep_Hour`, `Route`

### 5.3 Known data quirks (must be handled in ETL)
- BTS CSV rows end with a trailing comma, which creates an extra empty column. Select columns **by name** (do not rely on position).
- `CRSDepTime` is in **hhmm** format (e.g., `1530`) and can be `2400`. Use `Dep_Hour = floor(CRSDepTime / 100) % 24`.
- Delay-cause columns are only populated for flights with `ArrDel15 = 1`; otherwise null. Treat null as 0 when summing.
- `DepDelay`/`ArrDelay` are null for cancelled flights, and `ArrDelay` is null for diverted flights.
- Numeric fields come as strings like `"-3.00"`. Cast explicitly.

---

## 6. System Architecture

```
  BTS Flight CSVs (48 files, 12.5 GB)             RDBMS carrier / airport lookup tables
            │ (hdfs dfs -put)                                  │ (Apache Sqoop, JDBC)
            ▼                                                  ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │                        Hadoop HDFS                               │
   │  /project/flights/raw/*.csv        (raw CSV)                     │
   │  /project/flights/parquet/         (cleaned, ~35 cols, by Year)  │
   │  /project/reference/               (carrier / airport lookups)   │
   └───────────────────────────────┬──────────────────────────────────┘
                                   ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │                Apache Spark (PySpark)                            │
   │  Step 1: CSV → Parquet (explicit columns, casts, Dep_Hour)       │
   │  Step 2: 7 aggregate tables (Q1–Q8) read from Parquet            │
   │  Step 3: MLlib delay classifier (optional extension)             │
   └───────────────┬─────────────────────────────┬────────────────────┘
                   │ small CSV exports           │ sampled predictions
                   ▼                             ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │                ClickHouse (MergeTree tables)                     │
   │  7 agg_* tables + ml_delay_predictions                           │
   └───────────────────────────────┬──────────────────────────────────┘
                                   ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │   Power BI (ClickHouse connector, DirectQuery; Import = fallback)│
   │   P1 Executive KPIs · P2 Root Causes & Seasonality ·             │
   │   P3 Route Corridors · P4 ML Delay Risk                          │
   └──────────────────────────────────────────────────────────────────┘

  Apache Airflow orchestrates: check_data → csv_to_parquet → spark_aggregates
                               → load_clickhouse → (train_model)
```

**Streaming (Kafka + Spark Structured Streaming) is NOT part of the build.** It appears only as a roadmap/extension slide.

---

## 7. Table Contract (shared by all members)

Full DDL is in `PROJECT_COMPLETION_PLAN.md`, Section 6.3. Column names and types below are fixed.

| # | Table | Grain | Columns |
|---|---|---|---|
| 1 | `agg_airline_performance` | Year × Airline | Year, Reporting_Airline, Airline_Name, Total_Flights, OnTime_Flights, Delayed_Flights, Delay_Rate_Pct, Avg_Dep_Delay_Min, Avg_Arr_Delay_Min, Cancelled_Flights, Cancellation_Rate_Pct |
| 2 | `agg_airport_performance` | Airport (all years) | Airport_Code, Airport_City, Airport_State, Total_Departures, Total_Arrivals, Dep_Delay_Rate_Pct, Arr_Delay_Rate_Pct, Avg_Dep_Delay_Min, Cancelled_Departures |
| 3 | `agg_delay_causes_monthly` | Year × Month | Year, Month, Total_Delay_Minutes, Carrier/Weather/NAS/Security/Late_Aircraft_Delay_Min, and Carrier/Weather/NAS/Security/Late_Aircraft_Pct |
| 4 | `agg_hourly_delays` | Scheduled dep. hour (0–23) | Dep_Hour, Total_Flights, Delayed_Flights, Delay_Probability_Pct, Avg_Delay_Minutes |
| 5 | `agg_route_traffic` | Directed route (Origin → Dest) | Origin, Dest, Route_Name, Flight_Count, Avg_AirTime_Min, Distance_Miles, Route_Delay_Rate_Pct |
| 6 | `agg_calendar_delays` **(new)** | Year × Month × Day_Of_Week | Year, Month, Day_Of_Week, Total_Flights, Delayed_Flights, Cancelled_Flights, Delay_Rate_Pct, Avg_Arr_Delay_Min |
| 7 | `agg_cancellation_reasons` **(new)** | Year × Airline × Cancellation code | Year, Reporting_Airline, Cancellation_Code, Cancellation_Reason, Cancelled_Flights |
| 8 | `ml_delay_predictions` | One sampled test flight (≤ 500k rows) | FlightDate, Reporting_Airline, Origin, Dest, Dep_Hour, Actual_Delayed, Predicted_Delayed, Delay_Probability, Risk_Category |

### 7.1 Metric definitions (use exactly these, so every member gets identical numbers)
- **Total_Flights** = all rows (including cancelled and diverted).
- **Operated flight** = `Cancelled = 0` AND `Diverted = 0` AND `ArrDel15` not null.
- **Delayed_Flights** = operated flights with `ArrDel15 = 1`. **OnTime_Flights** = operated flights with `ArrDel15 = 0`.
- **Delay_Rate_Pct** = Delayed / (Delayed + OnTime) × 100.
- **Cancellation_Rate_Pct** = Cancelled / Total_Flights × 100.
- **Dep_Delay_Rate_Pct / Arr_Delay_Rate_Pct** = share of flights with `DepDel15 = 1` / `ArrDel15 = 1`.
- **Avg_*_Delay_Min** = average of `DepDelayMinutes` / `ArrDelayMinutes` (early = 0) over operated flights.
- **Total_Delay_Minutes** = sum of the five cause columns (null → 0). **Cause_Pct** = cause minutes / Total_Delay_Minutes × 100.
- **Dep_Hour** = `floor(CRSDepTime / 100) % 24` (scheduled hour).
- **Season** (computed in Power BI): Dec–Feb = Winter, Mar–May = Spring, Jun–Aug = Summer, Sep–Nov = Fall.
- **Route** is directed: `JFK-LAX` and `LAX-JFK` are different rows.
- **Cancellation_Reason** mapping: A = Carrier, B = Weather, C = NAS, D = Security.

---

## 8. ML Specification (optional extension; cut first if behind)

| Item | Rule |
|---|---|
| **Task** | Binary classification: will the flight arrive ≥ 15 min late? |
| **Label** | `label = ArrDel15` |
| **Population** | Operated flights only (drop cancelled, diverted, and null `ArrDel15`) |
| **Allowed features** | `Month`, `DayOfWeek`, `Dep_Hour`, `Reporting_Airline`, `Origin`, `Dest`, `Distance`, `CRSElapsedTime` (all known before departure) |
| **Forbidden features (data leakage)** | `DepTime`, `DepDelay`, `DepDelayMinutes`, `DepDel15`, `ArrTime`, `ActualElapsedTime`, `AirTime`, `ArrDelay*`, all `*Delay` cause columns, `Cancelled`, `Diverted`, any taxi / wheels-off / wheels-on field |
| **Split** | Train on 2022–2023 (use a 10–20% random sample); test on 2024. Optional final check on 2025. |
| **Models** | `LogisticRegression` (primary, fast) and optionally `RandomForestClassifier` (≤ 30 trees, depth ≤ 8) |
| **Spark 3 API** | Use `OneHotEncoder` (**not** `OneHotEncoderEstimator`, which only exists in Spark 2.x). `StringIndexer(handleInvalid="keep")`. If a tree model uses indexed categoricals directly, set `maxBins ≥ 400` (airports ≈ 350). |
| **Metrics** | AUC-ROC (main), precision, recall, confusion matrix. Accuracy alone is misleading, since roughly 80% of flights are on time. |
| **Expected result** | Modest: AUC around 0.60–0.70 is normal for schedule-only features. A much higher AUC means leakage. |
| **Output** | `ml_delay_predictions`, a ≤ 500k-row sample of the test set. Risk_Category thresholds on probability: Low < 0.15, Medium 0.15–0.30, High ≥ 0.30 (adjust after viewing the distribution). |

---

## 9. Architectural Decisions & Rationale (for reviewers)

1. **Sqoop for relational reference data only.** Raw CSVs go in with `hdfs dfs -put`. Sqoop imports carrier / airport lookup tables from an RDBMS (PostgreSQL, or MySQL if already installed) into HDFS, because Sqoop is built for RDBMS ↔ HDFS transfer. Sqoop is a retired Apache project and often breaks on Hadoop 3 (missing JDBC / commons-lang jars), so it is **time-boxed to 2 hours**. If it fails, load the lookup CSV with `hdfs dfs -put` and state this honestly in the presentation.
2. **ClickHouse between Spark and Power BI.** Spark / Hive give 10–30 s latency per click. ClickHouse MergeTree tables give sub-second cross-filtering. Power BI has a built-in ClickHouse connector with DirectQuery; it needs the ClickHouse ODBC driver installed. The aggregate tables are small, so **Import mode is a safe fallback**.
3. **Parquet intermediate layer.** Reading 12.5 GB of CSV on every run costs ~30 min. Convert once to partitioned Parquet (~1.5–2 GB) and run every later job from it.
4. **Spark → ClickHouse via small CSV exports.** Aggregates are tiny (hundreds to thousands of rows). Export with `toPandas().to_csv()` and load with `clickhouse-client … FORMAT CSVWithNames`. This avoids JDBC / unsigned-type (`UInt32`) mismatches.
5. **Dual environment: CentOS VM or Docker.** Run a health check first. CentOS 7/8 are end-of-life (default yum repos may fail) and CentOS 7's system Python is 3.6, which cannot install recent Airflow 2.x (needs ≥ 3.8). Use a Python ≥ 3.9 venv or the official Airflow Docker image. The Docker fallback must include HDFS, Spark, ClickHouse **and PostgreSQL**; Sqoop is VM-only.
6. **PySpark MLlib over Scikit-Learn.** Scikit-Learn is single-node; MLlib trains on Spark DataFrames. Training uses a sample to keep runs short.
7. **Two-phase development.** Build everything on one month (~550k rows, ~10 s per run), then switch the input path to all 48 files.
8. **Batch over streaming.** The batch pipeline is the mandatory deliverable. Kafka / Structured Streaming is roadmap only.

---

## 10. Technology Stack

| Technology | Layer | Role |
|---|---|---|
| **Linux (CentOS / Ubuntu / WSL2)** | Infrastructure | Execution environment (check disk ≥ 40 GB free, RAM ≥ 8 GB) |
| **Hadoop HDFS (2.7+ / 3.x)** | Storage | Raw CSV, Parquet, lookup tables (`dfs.replication = 1` on a single node) |
| **Apache Sqoop (1.4.7)** | Ingestion | JDBC import of carrier / airport lookup tables |
| **PostgreSQL (or MySQL)** | Relational source | Holds lookup tables for Sqoop |
| **Apache Spark 3.x / PySpark** | Processing & ML | CSV → Parquet, 7 aggregates, MLlib classifier |
| **ClickHouse (23.x+)** | Serving | Columnar store for dashboards |
| **Apache Airflow (2.x)** | Orchestration | DAG of `BashOperator` tasks; run in Python ≥ 3.9 |
| **Power BI Desktop (recent version)** | Visualization | 4-page dashboard; ClickHouse connector + ODBC driver |
| **Docker & Docker Compose** | Fail-safe | Clean HDFS / Spark / ClickHouse / PostgreSQL stack |
| *Kafka + Spark Structured Streaming* | *Roadmap only* | *Not built; shown on the future-work slide* |

---

## 11. Risks & Fallbacks

| Risk | Fallback |
|---|---|
| VM daemons fail | Switch to Docker Compose within the first 2 hours |
| Sqoop will not run | Stop after 2 hours; use `hdfs dfs -put`; be transparent in the deck |
| Airflow install fails on CentOS | Python ≥ 3.9 venv, or official Airflow Docker image (`standalone`) |
| Power BI cannot reach ClickHouse | Test in hours 0–3. Check ODBC driver, Power BI version, port 8123 / VM network. Fallback: export tables to CSV and use Import mode |
| Full-data Spark runs too slow | Always read Parquet, never raw CSV; select only needed columns |
| ML AUC looks low | Expected with leak-free features; report honestly. Do **not** add leaking features |
| Running behind schedule | Cut order: (1) ML page / model, (2) Airflow polish, (3) extra visuals. Never cut Spark ETL, ClickHouse, or the 4 core dashboards |
