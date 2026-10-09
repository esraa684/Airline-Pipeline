import os
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN

def create_presentation():
    prs = Presentation()
    prs.slide_width = Inches(13.333)  # 16:9 widescreen
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6]

    DARK_BLUE = RGBColor(16, 44, 87)
    ACCENT_BLUE = RGBColor(53, 95, 142)
    TEXT_DARK = RGBColor(33, 37, 41)
    TEXT_MUTED = RGBColor(108, 117, 125)
    WHITE = RGBColor(255, 255, 255)
    LIGHT_BG = RGBColor(248, 249, 250)

    slides_data = [
        {
            "num": 1,
            "title": "Flight & Airport Big Data Analytics Platform",
            "subtitle": "An End-to-End Distributed Analytics & Machine Learning Pipeline for U.S. Commercial Aviation",
            "owner": "Member 4 (Presentation Lead)",
            "bullets": [
                "National Telecommunication Institute (NTI) — Big Data Specialization Graduation Project",
                "Team Members & Assigned Roles:",
                " • Abd El-Wadoud Ahmad: Infrastructure, Storage & Orchestration (Member 1)",
                " • Aliaa Fayez: Spark Analytics & Distributed ETL (Member 2)",
                " • Mariam Mohamed: Machine Learning & Predictive Modeling (Member 3)",
                " • Esraa: Real-Time Streaming & BI Presentation Lead (Member 4)",
                "A complete Lambda Architecture integrating Batch, Real-Time Streaming, and Leak-Free AI."
            ]
        },
        {
            "num": 2,
            "title": "Business Problem & Motivation",
            "subtitle": "Why Big Data is Required to Tackle the $33B Flight Delay Crisis",
            "owner": "Member 4",
            "bullets": [
                "The Economic Cost: Flight delays and cancellations cost U.S. carriers, airports, and passengers over $33 Billion annually.",
                "Compounding Ripple Effects: An unmitigated 30-minute delay in the morning cascades across flight legs, resulting in cancelled evening returns.",
                "Why Traditional Tools Fail:",
                " • Microsoft Excel crashes at 1,048,576 rows (Our dataset is 27.4 Million rows).",
                " • Single-threaded Python/Pandas exhausts 35–45 GB of RAM, causing severe Out-Of-Memory (OOM) failures.",
                "The Big Data Solution: Scalable distributed storage (HDFS), in-memory compute (Spark), sub-second OLAP serving (ClickHouse), and interactive visual intelligence."
            ]
        },
        {
            "num": 3,
            "title": "Dataset Architecture & Volume",
            "subtitle": "U.S. Bureau of Transportation Statistics (BTS) On-Time Performance (2022–2025)",
            "owner": "Member 1",
            "bullets": [
                "Dataset Scope: 4 Full Years (2022, 2023, 2024, 2025) comprising 48 monthly raw CSV datasets.",
                "Scale & Dimensions:",
                " • Total Volume: 12.50 Gigabytes of uncompressed text.",
                " • Total Flight Records: ~27,400,000 commercial flights.",
                " • Attribute Richness: 109 raw columns per flight record (Carrier, Tail#, Times, Delays, Causes, Cancellations).",
                "Automated Ingestion Pipeline: Ingested programmatically and distributed across Hadoop HDFS (/project/flights/raw/)."
            ]
        },
        {
            "num": 4,
            "title": "End-to-End Lambda Architecture",
            "subtitle": "Decoupled Storage & Compute Combining Batch Analytics and Real-Time Speed Layers",
            "owner": "Member 1",
            "bullets": [
                "Batch Layer: Raw CSVs → Hadoop HDFS → Apache Spark 3.3.0 Batch ETL → Snappy Parquet → ClickHouse OLAP → Power BI.",
                "Speed Layer (Streaming): Event Producer → Apache Kafka (KRaft mode) → Spark Structured Streaming → ClickHouse → Grafana Live Dashboard.",
                "Serving Layer (ClickHouse): Columnar OLAP engine responding to analytical aggregation queries in 7–15 milliseconds.",
                "Orchestration: Apache Airflow automated DAG managing dependency execution across the entire data lifecycle."
            ]
        },
        {
            "num": 5,
            "title": "Distributed Storage: HDFS vs. MinIO",
            "subtitle": "Architectural Evaluation of Distributed Block Storage and Cloud-Native Object Stores",
            "owner": "Member 1",
            "bullets": [
                "Hadoop Distributed File System (HDFS):",
                " • Master-worker architecture (NameNode manages metadata, DataNodes store 128 MB blocks).",
                " • Built-in fault tolerance and rack awareness for high-throughput sequential reads.",
                "MinIO Cloud-Native Object Storage (Modern Alternative):",
                " • High-performance, S3-compatible cloud-native object store.",
                " • Eliminates NameNode metadata bottlenecks; native format for modern Data Lakehouse architectures.",
                "Key Takeaway: Our Snappy Parquet layer seamlessly supports both HDFS (hdfs://) and MinIO (s3a://) with zero code refactoring."
            ]
        },
        {
            "num": 6,
            "title": "Distributed Processing & Spark ETL",
            "subtitle": "Columnar Conversion, Snappy Compression, and Feature Engineering at Scale",
            "owner": "Member 2",
            "bullets": [
                "Storage Optimization (CSV to Parquet):",
                " • Uncompressed CSV (12.5 GB) → Snappy-compressed Apache Parquet (~1.5 GB).",
                " • Achieved over 90% disk space compression and 100x faster analytical read speeds.",
                " • Partitioned by Year (Year=2022 ... 2025) enabling automatic partition pruning.",
                "Data Cleaning & Transformation Logic:",
                " • Explicit schema enforcement: retained 35 critical columns out of 109, stripping trailing commas.",
                " • Feature derivation: Dep_Hour = floor(CRSDepTime / 100) % 24 and directed Route = Origin - Dest.",
                " • Null handling: delay causes imputed to 0 for on-time operations."
            ]
        },
        {
            "num": 7,
            "title": "Core Operational Findings (Q1, Q2, Q7)",
            "subtitle": "Carrier Rankings, Congested Bottlenecks, and Busiest Route Corridors",
            "owner": "Member 2",
            "bullets": [
                "Q1 — Airline Delay Rankings: American Airlines (29.37% delay rate) and JetBlue (29.03%) experience the highest delay rates, averaging 23–26 minutes per late flight.",
                "Q2 — Congested Airport Hubs: Regional feeder hubs suffer the longest delays: Elmira/Corning (ELM) exhibits a 41.33% departure delay rate with an average delay of 83.9 minutes.",
                "Q7 — Busiest Flight Corridors:",
                " • #1 High-Frequency Corridor: Kahului (OGG) ↔ Honolulu (HNL) with ~2,000 monthly flights.",
                " • #1 Continental Route: Los Angeles (LAX) → San Francisco (SFO) with an alarming 33.01% delay rate due to coastal weather and runway throttles."
            ]
        },
        {
            "num": 8,
            "title": "Temporal & Root Cause Analysis (Q3, Q4, Q5, Q6)",
            "subtitle": "Hourly Delay Cascades, 5-Cause Breakdown, and Cancellation Drivers",
            "owner": "Member 2",
            "bullets": [
                "Q3 — Hourly Compounding Cascade: Delays start at baseline (<12%) at 6:00 AM and compound continuously, peaking between 6:00 PM and 9:00 PM at nearly 29%.",
                "Q4 — Dominant Delay Causes Breakdown:",
                " • Late Aircraft Delay (38.95%): The #1 dominant driver due to inbound late planes.",
                " • Carrier Operations (32.62%): Mechanical maintenance, crew rostering, boarding.",
                " • National Airspace System (17.92%): ATC traffic throttles and runway congestion.",
                " • Severe Weather (10.27%): Meteorological ground stops. Security: 0.24%.",
                "Q5 & Q6 — Calendar Trends & Cancellations: Tuesday (26.9%) and Friday (25.7%) suffer peak weekly delays. Winter cancellations are dominated by Severe Weather (59.3%) and Carrier issues (37.9%)."
            ]
        },
        {
            "num": 9,
            "title": "Machine Learning: Leak-Free AI Formulation",
            "subtitle": "Pre-Departure Delay Risk Classification with Zero Future Data Leakage",
            "owner": "Member 3 (ML Engineer)",
            "bullets": [
                "Task Formulation: Binary classification predicting whether a flight will arrive ≥ 15 minutes late (ArrDel15 = 1). Population: Operated flights only.",
                "The Strict Zero-Leakage Guarantee:",
                " • Forbidden Features: DepDelay, ActualElapsedTime, AirTime, ArrTime, and delay-cause minutes are strictly excluded (unknown at departure).",
                " • 8 Allowed Pre-Departure Features: Month, DayOfWeek, Dep_Hour, Reporting_Airline, Origin, Dest, Distance, CRSElapsedTime.",
                "Spark MLlib Pipeline: StringIndexer (handleInvalid='keep') → OneHotEncoder → VectorAssembler → Estimator."
            ]
        },
        {
            "num": 10,
            "title": "ML Model Benchmark & Evaluation",
            "subtitle": "Why Random Forest Was Rejected; Logistic Regression vs. GBT Comparison",
            "owner": "Member 3",
            "bullets": [
                "The Rejection of Random Forest on Big Data:",
                " • Origin and Dest have 350+ categorical levels (maxBins >= 400).",
                " • Building 50–100 parallel trees causes exponential memory consumption and Spark executor Out-Of-Memory (OOM) failures.",
                "Models Evaluated:",
                " • Model 1: Logistic Regression (L-BFGS) — Fast, convex, scalable baseline.",
                " • Model 2: Gradient Boosted Trees (GBTClassifier, maxDepth=5) — Shallow sequential boosted trees.",
                "Evaluation Results: AUC-ROC = 0.66 | Accuracy = 78.4%. An AUC of 0.60–0.70 is the recognized aviation standard for schedule-only features, proving genuine scientific integrity without leakage."
            ]
        },
        {
            "num": 11,
            "title": "Executive Dashboards Showcase",
            "subtitle": "Interactive Power BI Executive Report & Live Grafana Streaming Monitor",
            "owner": "Member 4",
            "bullets": [
                "Power BI 4-Page Analytics Report (Batch Layer):",
                " • Page 1: Executive Operations Overview (KPI cards, reliability rankings, carrier performance).",
                " • Page 2: Root Causes & Seasonality (Cause donut chart, hourly delay curve, weekly surge).",
                " • Page 3: Route Corridor Intelligence (Top routes matrix, distance vs. delay scatter plot).",
                " • Page 4: AI/ML Delay Risk Predictor (Risk category segmentation: Low <15%, Medium 15–30%, High ≥30%).",
                "Grafana Real-Time Dashboard (Speed Layer): Live streaming metrics from ClickHouse (flights processed/sec, real-time delay rates)."
            ]
        },
        {
            "num": 12,
            "title": "Orchestration, Conclusions & Future Work",
            "subtitle": "Pipeline Automation, Key Achievements, and Production Roadmap",
            "owner": "Member 4",
            "bullets": [
                "Pipeline Automation (Apache Airflow): Automated 5-stage BashOperator DAG with automatic retries and visual green tree-view verification.",
                "Core Engineering Milestones:",
                " • Ingested and compressed 12.5 GB of raw aviation data into a sub-second OLAP platform.",
                " • Built a fully functioning Lambda Architecture combining batch analytical rigor and real-time event streaming.",
                " • Deployed leak-free machine learning providing pre-departure risk assessment.",
                "Future Roadmap: Kubernetes (EKS/GKE) cluster deployment with Helm charts; Delta Lake ACID transaction integration on S3/MinIO."
            ]
        }
    ]

    for item in slides_data:
        slide = prs.slides.add_slide(blank_layout)

        # Header Banner
        header_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.5), Inches(11.7), Inches(1.3))
        tf = header_box.text_frame
        tf.word_wrap = True
        
        p_title = tf.paragraphs[0]
        p_title.text = f"Slide {item['num']}: {item['title']}"
        p_title.font.name = "Calibri"
        p_title.font.size = Pt(24)
        p_title.font.bold = True
        p_title.font.color.rgb = DARK_BLUE

        p_sub = tf.add_paragraph()
        p_sub.text = f"{item['subtitle']}  |  Owner: {item['owner']}"
        p_sub.font.name = "Calibri"
        p_sub.font.size = Pt(13)
        p_sub.font.color.rgb = ACCENT_BLUE

        # Content Box
        content_box = slide.shapes.add_textbox(Inches(0.8), Inches(1.9), Inches(11.7), Inches(5.0))
        tf_c = content_box.text_frame
        tf_c.word_wrap = True

        for idx, bullet in enumerate(item['bullets']):
            p = tf_c.paragraphs[0] if idx == 0 else tf_c.add_paragraph()
            p.text = bullet
            p.font.name = "Calibri"
            p.font.size = Pt(16)
            p.font.color.rgb = TEXT_DARK
            p.space_after = Pt(12)

    out_file = r"c:\Users\Abdelwadoud\Documents\BigData-NTI-Files\BigDataFinalProject\Flight_Analytics_Platform_Presentation.pptx"
    prs.save(out_file)
    print(f"Presentation successfully created at: {out_file}")

if __name__ == "__main__":
    create_presentation()
