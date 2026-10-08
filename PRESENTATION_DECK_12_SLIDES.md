# Master 12-Slide Presentation Deck
## Flight & Airport Big Data Analytics Platform
### National Telecommunication Institute (NTI) — Big Data Specialization

---

## 📋 Slide Ownership Matrix

| Slide # | Slide Title | Owner | Core Content |
| :---: | :--- | :---: | :--- |
| **1** | Title & Team Specialization | **Member 4** | Project title, university, NTI program, team names & roles. |
| **2** | Business Problem & Motivation | **Member 4** | The $33B delay problem, operational impact, why Big Data. |
| **3** | Dataset Architecture & Scale | **Member 1** | BTS 2022–2025, 27.4M flights, 109 attributes, 12.5 GB volume. |
| **4** | End-to-End Lambda Architecture | **Member 1** | Batch (HDFS $\rightarrow$ Spark $\rightarrow$ ClickHouse $\rightarrow$ PBI) + Speed (Kafka $\rightarrow$ Grafana). |
| **5** | Storage: HDFS & MinIO Object Storage | **Member 1** | Distributed block storage vs. modern cloud-native S3 object store. |
| **6** | Distributed Processing & Spark ETL | **Member 2** | Snappy Parquet (90% compression), schema casting, `Dep_Hour` derivation. |
| **7** | Core Operational Findings (Q1, Q2, Q7) | **Member 2** | Worst airlines, airport bottlenecks, highest-traffic route corridors. |
| **8** | Temporal & Root Cause Analysis (Q3, Q4, Q5, Q6) | **Member 2** | Compounding peak hours, 5 delay causes, cancellation drivers. |
| **9** | Machine Learning Formulation (Leak-Free) | **Member 3** | Binary classification (`ArrDel15`), strict pre-departure feature selection. |
| **10** | ML Evaluation: GBT & Logistic Regression | **Member 3** | Why Random Forest was rejected, AUC-ROC (0.66), confusion matrix. |
| **11** | Executive Dashboards Showcase | **Member 4** | 4-Page Power BI report (Batch) + Grafana Live Dashboard (Real-Time). |
| **12** | Airflow Orchestration & Future Streaming | **Member 4** | Automated DAG workflow, production conclusions, streaming roadmap. |

---

## Slide 1: Title & Team Specialization
* **Slide Owner:** Member 4 (Presentation Lead)
* **Title:** Flight & Airport Big Data Analytics Platform
* **Subtitle:** An End-to-End Distributed Analytics & Machine Learning Pipeline for U.S. Aviation Operations
* **Program:** National Telecommunication Institute (NTI) — Big Data Specialization Graduation Project
* **Team Members & Specializations:**
  - **Abd El-Wadoud Ahmad:** Infrastructure, Distributed Storage & Orchestration (Member 1)
  - **Aliaa Fayez:** Spark Analytics, Distributed ETL & Business Intelligence (Member 2)
  - **Mariam Mohamed:** Machine Learning & Predictive Modeling (Member 3)
  - **Esraa:** Real-Time Streaming, Dashboarding & Presentation Lead (Member 4)

---

## Slide 2: Business Problem & Motivation
* **Slide Owner:** Member 4
* **The Economic Impact:**
  - Flight delays and cancellations cost U.S. airlines, passengers, and airports **over $33 Billion annually**.
  - Ripple effect: A 30-minute delay in the morning cascades into cancelled return legs by evening.
* **Why Traditional Analytics Fails:**
  - Microsoft Excel crashes at **1,048,576 rows**; our dataset contains **27,400,000+ rows**.
  - Single-node Python Pandas demands **35–45 GB of RAM**, causing catastrophic Out-Of-Memory (OOM) failures.
* **Our Project Mission:**
  - Build an industrial-grade Big Data platform capable of ingesting, optimizing, and querying 4 years of flight records in milliseconds, coupled with AI to predict delays before departure.

---

## Slide 3: Dataset Architecture & Scale
* **Slide Owner:** Member 1
* **Source:** U.S. Bureau of Transportation Statistics (BTS) On-Time Performance Reporting Carrier data.
* **Scope & Timeframe:** 4 Complete Years (2022 to 2025) — 48 Monthly Datasets.
* **Key Statistics:**
  - **Raw Text Volume:** **12.50 Gigabytes** of raw CSV files.
  - **Total Flights Analyzed:** **~27,400,000 commercial flights**.
  - **Dimensionality:** **109 attributes per flight** (Flight date, tail number, scheduled & actual times, delay breakdown by minutes, cancellation codes, distance, elapsed airtime).
* **Storage Ingestion:**
  - Automated download $\rightarrow$ staged in Hadoop Distributed File System (`/project/flights/raw/`).

---

## Slide 4: End-to-End Architecture (Lambda Pattern)
* **Slide Owner:** Member 1
* **Architecture Diagram:**
  - **Batch Layer:** Raw CSVs $\rightarrow$ HDFS Storage $\rightarrow$ Apache Spark 3.3.0 Batch ETL $\rightarrow$ Partitioned Snappy Parquet $\rightarrow$ ClickHouse OLAP $\rightarrow$ Power BI.
  - **Speed Layer:** Kafka Producer $\rightarrow$ Apache Kafka (KRaft mode) $\rightarrow$ Spark Structured Streaming $\rightarrow$ ClickHouse (`flights_processed`) $\rightarrow$ Grafana Live Dashboard.
  - **Orchestration:** Apache Airflow DAG (`check_data` $\rightarrow$ `csv_to_parquet` $\rightarrow$ `spark_aggregates` $\rightarrow$ `load_clickhouse` $\rightarrow$ `train_model`).
* **Key Architecture Win:** Storage and compute are completely decoupled, supporting both sub-second historical analytics and real-time live event monitoring.

---

## Slide 5: Storage Layer: HDFS & MinIO Object Storage
* **Slide Owner:** Member 1
* **Traditional Distributed Storage (HDFS):**
  - NameNode coordinates filesystem namespace; DataNodes store raw blocks across nodes.
  - Replicated storage protects against physical disk failures.
* **Modern Cloud-Native Storage (MinIO S3):**
  - High-performance, S3-compatible Object Storage for modern Lakehouse architectures.
  - Eliminates NameNode metadata bottlenecks; provides native S3 API access for Parquet files.
* **Comparison Matrix:**
  - *HDFS:* Proven on-premise standard, block-based POSIX.
  - *MinIO:* Cloud-native, zero-overhead Kubernetes containerization, S3 SDK compatible.

---

## Slide 6: Distributed Processing & Spark ETL
* **Slide Owner:** Member 2
* **Storage Optimization (CSV $\rightarrow$ Parquet):**
  - Converted uncompressed CSVs to **Snappy-compressed Apache Parquet**.
  - Reduced storage footprint from **12.5 GB to ~1.5 GB (>90% compression ratio)**.
  - Partitioned by `Year` (`Year=2022`, `Year=2023`, `Year=2024`, `Year=2025`) for partition pruning.
* **Data Cleaning & Transformations in PySpark:**
  - Selected explicit schema (35 essential columns from 109, eliminating trailing commas).
  - Engineered scheduled departure hour: `Dep_Hour = floor(CRSDepTime / 100) % 24`.
  - Derived directed corridor route: `Route = Origin - Dest`.
  - Handled nulls in delay causes (`na.fill(0)` for on-time flights).

---

## Slide 7: Core Operational Findings (Q1, Q2, Q7)
* **Slide Owner:** Member 2
* **Q1 — Airline Delay Rankings:**
  - **Worst Airlines:** American Airlines (**29.37%** delay rate) and JetBlue (**29.03%** delay rate).
  - Average arrival delay for late flights ranges from **23.6 to 26.7 minutes**.
* **Q2 — Congested Airport Hubs:**
  - Regional feeder bottlenecks suffer the longest delays: **Elmira/Corning (ELM)** has a **41.33%** departure delay rate with an average delay duration of **83.9 minutes**!
* **Q7 — Busiest Flight Corridors:**
  - **#1 Domestic Route:** Hawaiian inter-island shuttle (Kahului `OGG` $\leftrightarrow$ Honolulu `HNL`) with ~2,000 monthly flights.
  - **#1 Continental Route:** Los Angeles (`LAX`) $\rightarrow$ San Francisco (`SFO`) with an alarming **33.01% delay rate** due to morning coastal fog throttles.

---

## Slide 8: Temporal & Root Cause Analysis (Q3, Q4, Q5, Q6)
* **Slide Owner:** Member 2
* **Q3 — Compounding Peak Hours:**
  - Lowest delays occur at 6:00 AM (<12%). Delays compound continuously across the day, peaking between **6:00 PM and 9:00 PM (Hour 18–20) at ~28.8% delay probability**.
* **Q4 — Delay Causes Breakdown:**
  - **Late Aircraft Delay (38.95%):** The #1 dominant driver (inbound planes arriving late).
  - **Carrier Operational Delay (32.62%):** Maintenance, crew rostering, baggage handling.
  - **National Airspace System (17.92%):** Air traffic control volume management.
  - **Severe Weather (10.27%):** Winter storms and blizzards.
* **Q5 & Q6 — Seasonality & Cancellations:**
  - Tuesday (**26.88%**) and Friday (**25.69%**) experience the highest delay rates.
  - Winter cancellations are dominated by **Severe Weather (59.3%)**, followed by **Carrier Mechanical issues (37.9%)**.

---

## Slide 9: Machine Learning Formulation (Leak-Free AI)
* **Slide Owner:** Member 3 (ML Engineer)
* **Problem Definition:**
  - **Task:** Binary Classification — Predict if a flight will arrive $\ge$ 15 minutes late (`ArrDel15 = 1`).
  - **Population:** Operated flights only (cancelled/diverted flights excluded).
* **Strict Leak-Free Principle (Zero Data Leakage):**
  - **Forbidden Features:** `DepDelay`, `DepTime`, `AirTime`, `ActualElapsedTime`, and delay causes are strictly excluded because they are impossible to know before departure!
* **The 8 Pre-Departure Features Used:**
  - `Month`, `DayOfWeek`, `Dep_Hour`, `Reporting_Airline`, `Origin`, `Dest`, `Distance`, `CRSElapsedTime`.
* **Feature Pipeline:**
  - `StringIndexer(handleInvalid="keep")` $\rightarrow$ `OneHotEncoder` $\rightarrow$ `VectorAssembler`.

---

## Slide 10: ML Evaluation: GBT & Logistic Regression
* **Slide Owner:** Member 3
* **Why Random Forest Was Deliberately Rejected:**
  - Random Forest on 350+ categorical origin/destination airports causes exponential tree split complexity (`maxBins >= 400`), resulting in Spark executor Out-Of-Memory (OOM) crashes and slow execution.
* **Model Benchmark & Comparison:**
  - **Logistic Regression (L-BFGS):** Fast, scalable, convex optimization baseline.
  - **Gradient Boosted Trees (GBTClassifier):** Depth-bounded ($d \le 5$) boosted trees capturing non-linear feature interactions.
* **Results & Scientific Interpretation:**
  - **AUC-ROC Score:** **0.66** | **Accuracy:** **78.4%**.
  - In aviation research, schedule-only features inherently yield an AUC between 0.60 and 0.70 because mid-flight en-route weather cannot be predicted at gate pushback. An AUC of 0.66 demonstrates scientific integrity and zero leakage.

---

## Slide 11: Executive Dashboards Showcase
* **Slide Owner:** Member 4
* **Power BI 4-Page Analytics Report (Batch Layer):**
  - **Page 1: Executive Overview:** High-level KPIs, airline on-time reliability ranking, airport throughput.
  - **Page 2: Delay Root Causes & Seasonality:** Cause breakdown donut chart, monthly seasonality, hourly compounding curves.
  - **Page 3: Route Corridor Intelligence:** Origin-destination corridor traffic volume and delay rate rankings.
  - **Page 4: AI/ML Delay Risk Predictor:** Risk category segmentation (Low <15%, Medium 15–30%, High $\ge$30%).
* **Grafana Real-Time Dashboard (Speed Layer):**
  - Connected directly to ClickHouse serving live Kafka streaming metrics (incoming flight rates, active delays).

---

## Slide 12: Orchestration, Future Work & Conclusion
* **Slide Owner:** Member 4
* **Workflow Automation (Apache Airflow):**
  - Automated 5-stage pipeline executed via linear BashOperators with automatic retries and visual tree-view monitoring.
* **Key Project Achievements:**
  - Successfully scaled from 12.5 GB of raw CSV chaos to sub-second (7 ms) ClickHouse analytical queries.
  - True Lambda Architecture demonstrated combining batch (HDFS/Spark/ClickHouse) and streaming (Kafka/Grafana).
  - Leak-free ML classifier providing actionable operational risk ratings for passengers and dispatchers.
* **Future Work:**
  - Production deployment on Kubernetes using Helm.
  - Delta Lake / Apache Iceberg integration on MinIO for ACID streaming transactions.
