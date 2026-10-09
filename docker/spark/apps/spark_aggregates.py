import os
import shutil
import time
from pyspark.sql import SparkSession, functions as F

PARQUET_PATH = "hdfs://namenode:9000/project/flights/parquet"
OUTPUT_DIR = "/opt/spark-apps/output"

def export_table_csv(df, table_name):
    """
    Exports a Spark DataFrame to a single clean CSV file with headers,
    using pure native Spark (zero pandas dependency).
    """
    temp_path = f"{OUTPUT_DIR}/_temp_{table_name}"
    final_csv = f"{OUTPUT_DIR}/{table_name}.csv"
    
    # Write as single partition CSV
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    
    # Locate the generated part-*.csv file and move it to final_csv
    if os.path.exists(temp_path):
        for fname in os.listdir(temp_path):
            if fname.startswith("part-") and fname.endswith(".csv"):
                src = os.path.join(temp_path, fname)
                shutil.copyfile(src, final_csv)
                break
        shutil.rmtree(temp_path, ignore_errors=True)
    
    count = df.count()
    print(f" -> Successfully exported {table_name}.csv ({count} rows)")
    return final_csv

def main():
    print("=" * 70)
    print("Starting Spark Analytical Aggregations Job (Tables 1 - 7)...")
    print("=" * 70)
    
    start_time = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("FlightAnalytics_Aggregations")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    print(f"Reading cleaned Parquet data from {PARQUET_PATH}...")
    df = spark.read.parquet(PARQUET_PATH)

    # -------------------------------------------------------------------------
    # Base columns & definitions
    # -------------------------------------------------------------------------
    # Operated = Cancelled == 0 AND Diverted == 0 AND ArrDel15 IS NOT NULL
    df_base = df.withColumn(
        "is_operated",
        (F.col("Cancelled") == 0) & (F.col("Diverted") == 0) & F.col("ArrDel15").isNotNull()
    )

    # -------------------------------------------------------------------------
    # Table 1: agg_airline_performance (Q1, Q6, Q8)
    # -------------------------------------------------------------------------
    print("Computing Table 1: agg_airline_performance...")
    t1 = df_base.groupBy("Year", "Reporting_Airline").agg(
        F.first("Reporting_Airline").alias("Airline_Name"),
        F.count("*").alias("Total_Flights"),
        F.sum(F.when(F.col("is_operated") & (F.col("ArrDel15") == 0), 1).otherwise(0)).alias("OnTime_Flights"),
        F.sum(F.when(F.col("is_operated") & (F.col("ArrDel15") == 1), 1).otherwise(0)).alias("Delayed_Flights"),
        F.sum(F.when(F.col("Cancelled") == 1, 1).otherwise(0)).alias("Cancelled_Flights"),
        F.avg(F.when(F.col("is_operated"), F.coalesce(F.col("DepDelayMinutes"), F.lit(0)))).alias("Avg_Dep_Delay_Min"),
        F.avg(F.when(F.col("is_operated"), F.coalesce(F.col("ArrDelayMinutes"), F.lit(0)))).alias("Avg_Arr_Delay_Min")
    ).withColumn(
        "Delay_Rate_Pct",
        F.when(
            (F.col("Delayed_Flights") + F.col("OnTime_Flights")) > 0,
            (F.col("Delayed_Flights") / (F.col("Delayed_Flights") + F.col("OnTime_Flights"))) * 100.0
        ).otherwise(0.0)
    ).withColumn(
        "Cancellation_Rate_Pct",
        F.when(F.col("Total_Flights") > 0, (F.col("Cancelled_Flights") / F.col("Total_Flights")) * 100.0).otherwise(0.0)
    ).select(
        F.col("Year").cast("int"),
        F.col("Reporting_Airline").cast("string"),
        F.col("Airline_Name").cast("string"),
        F.col("Total_Flights").cast("int"),
        F.col("OnTime_Flights").cast("int"),
        F.col("Delayed_Flights").cast("int"),
        F.round(F.col("Delay_Rate_Pct"), 2).cast("float").alias("Delay_Rate_Pct"),
        F.round(F.col("Avg_Dep_Delay_Min"), 2).cast("float").alias("Avg_Dep_Delay_Min"),
        F.round(F.col("Avg_Arr_Delay_Min"), 2).cast("float").alias("Avg_Arr_Delay_Min"),
        F.col("Cancelled_Flights").cast("int"),
        F.round(F.col("Cancellation_Rate_Pct"), 2).cast("float").alias("Cancellation_Rate_Pct")
    ).na.fill(0)
    export_table_csv(t1, "agg_airline_performance")

    # -------------------------------------------------------------------------
    # Table 2: agg_airport_performance (Q2, Q6)
    # -------------------------------------------------------------------------
    print("Computing Table 2: agg_airport_performance...")
    dep_stats = df_base.groupBy("Origin").agg(
        F.first("OriginCityName").alias("Airport_City"),
        F.first("OriginState").alias("Airport_State"),
        F.count("*").alias("Total_Departures"),
        F.sum(F.when(F.col("is_operated") & (F.col("DepDel15") == 1), 1).otherwise(0)).alias("Delayed_Departures"),
        F.sum(F.when(F.col("is_operated"), 1).otherwise(0)).alias("Operated_Departures"),
        F.avg(F.when(F.col("is_operated"), F.coalesce(F.col("DepDelayMinutes"), F.lit(0)))).alias("Avg_Dep_Delay_Min"),
        F.sum(F.when(F.col("Cancelled") == 1, 1).otherwise(0)).alias("Cancelled_Departures")
    ).withColumnRenamed("Origin", "Airport_Code")

    arr_stats = df_base.groupBy("Dest").agg(
        F.count("*").alias("Total_Arrivals"),
        F.sum(F.when(F.col("is_operated") & (F.col("ArrDel15") == 1), 1).otherwise(0)).alias("Delayed_Arrivals"),
        F.sum(F.when(F.col("is_operated"), 1).otherwise(0)).alias("Operated_Arrivals")
    ).withColumnRenamed("Dest", "Airport_Code_Arr")

    t2 = dep_stats.join(arr_stats, dep_stats.Airport_Code == arr_stats.Airport_Code_Arr, "left").withColumn(
        "Dep_Delay_Rate_Pct",
        F.when(F.col("Operated_Departures") > 0, (F.col("Delayed_Departures") / F.col("Operated_Departures")) * 100.0).otherwise(0.0)
    ).withColumn(
        "Arr_Delay_Rate_Pct",
        F.when(F.col("Operated_Arrivals") > 0, (F.col("Delayed_Arrivals") / F.col("Operated_Arrivals")) * 100.0).otherwise(0.0)
    ).select(
        F.col("Airport_Code").cast("string"),
        F.coalesce(F.col("Airport_City"), F.lit("")).cast("string").alias("Airport_City"),
        F.coalesce(F.col("Airport_State"), F.lit("")).cast("string").alias("Airport_State"),
        F.coalesce(F.col("Total_Departures"), F.lit(0)).cast("int").alias("Total_Departures"),
        F.coalesce(F.col("Total_Arrivals"), F.lit(0)).cast("int").alias("Total_Arrivals"),
        F.round(F.col("Dep_Delay_Rate_Pct"), 2).cast("float").alias("Dep_Delay_Rate_Pct"),
        F.round(F.col("Arr_Delay_Rate_Pct"), 2).cast("float").alias("Arr_Delay_Rate_Pct"),
        F.round(F.col("Avg_Dep_Delay_Min"), 2).cast("float").alias("Avg_Dep_Delay_Min"),
        F.coalesce(F.col("Cancelled_Departures"), F.lit(0)).cast("int").alias("Cancelled_Departures")
    ).na.fill(0)
    export_table_csv(t2, "agg_airport_performance")

    # -------------------------------------------------------------------------
    # Table 3: agg_delay_causes_monthly (Q4)
    # -------------------------------------------------------------------------
    print("Computing Table 3: agg_delay_causes_monthly...")
    t3 = df_base.filter(F.col("is_operated") & (F.col("ArrDel15") == 1)).groupBy("Year", "Month").agg(
        F.sum(F.coalesce(F.col("CarrierDelay"), F.lit(0))).alias("Carrier_Delay_Min"),
        F.sum(F.coalesce(F.col("WeatherDelay"), F.lit(0))).alias("Weather_Delay_Min"),
        F.sum(F.coalesce(F.col("NASDelay"), F.lit(0))).alias("NAS_Delay_Min"),
        F.sum(F.coalesce(F.col("SecurityDelay"), F.lit(0))).alias("Security_Delay_Min"),
        F.sum(F.coalesce(F.col("LateAircraftDelay"), F.lit(0))).alias("Late_Aircraft_Delay_Min")
    ).withColumn(
        "Total_Delay_Minutes",
        F.col("Carrier_Delay_Min") + F.col("Weather_Delay_Min") + F.col("NAS_Delay_Min") + F.col("Security_Delay_Min") + F.col("Late_Aircraft_Delay_Min")
    ).withColumn(
        "Carrier_Pct", F.when(F.col("Total_Delay_Minutes") > 0, (F.col("Carrier_Delay_Min") / F.col("Total_Delay_Minutes")) * 100.0).otherwise(0.0)
    ).withColumn(
        "Weather_Pct", F.when(F.col("Total_Delay_Minutes") > 0, (F.col("Weather_Delay_Min") / F.col("Total_Delay_Minutes")) * 100.0).otherwise(0.0)
    ).withColumn(
        "NAS_Pct", F.when(F.col("Total_Delay_Minutes") > 0, (F.col("NAS_Delay_Min") / F.col("Total_Delay_Minutes")) * 100.0).otherwise(0.0)
    ).withColumn(
        "Security_Pct", F.when(F.col("Total_Delay_Minutes") > 0, (F.col("Security_Delay_Min") / F.col("Total_Delay_Minutes")) * 100.0).otherwise(0.0)
    ).withColumn(
        "Late_Aircraft_Pct", F.when(F.col("Total_Delay_Minutes") > 0, (F.col("Late_Aircraft_Delay_Min") / F.col("Total_Delay_Minutes")) * 100.0).otherwise(0.0)
    ).select(
        F.col("Year").cast("int"),
        F.col("Month").cast("int"),
        F.col("Total_Delay_Minutes").cast("long"),
        F.col("Carrier_Delay_Min").cast("long"),
        F.col("Weather_Delay_Min").cast("long"),
        F.col("NAS_Delay_Min").cast("long"),
        F.col("Security_Delay_Min").cast("long"),
        F.col("Late_Aircraft_Delay_Min").cast("long"),
        F.round(F.col("Carrier_Pct"), 2).cast("float").alias("Carrier_Pct"),
        F.round(F.col("Weather_Pct"), 2).cast("float").alias("Weather_Pct"),
        F.round(F.col("NAS_Pct"), 2).cast("float").alias("NAS_Pct"),
        F.round(F.col("Security_Pct"), 2).cast("float").alias("Security_Pct"),
        F.round(F.col("Late_Aircraft_Pct"), 2).cast("float").alias("Late_Aircraft_Pct")
    ).na.fill(0)
    export_table_csv(t3, "agg_delay_causes_monthly")

    # -------------------------------------------------------------------------
    # Table 4: agg_hourly_delays (Q3)
    # -------------------------------------------------------------------------
    print("Computing Table 4: agg_hourly_delays...")
    t4 = df_base.groupBy("Dep_Hour").agg(
        F.count("*").alias("Total_Flights"),
        F.sum(F.when(F.col("is_operated") & (F.col("ArrDel15") == 1), 1).otherwise(0)).alias("Delayed_Flights"),
        F.avg(F.when(F.col("is_operated"), F.coalesce(F.col("ArrDelayMinutes"), F.lit(0)))).alias("Avg_Delay_Minutes")
    ).withColumn(
        "Delay_Probability_Pct",
        F.when(F.col("Total_Flights") > 0, (F.col("Delayed_Flights") / F.col("Total_Flights")) * 100.0).otherwise(0.0)
    ).select(
        F.col("Dep_Hour").cast("int"),
        F.col("Total_Flights").cast("int"),
        F.col("Delayed_Flights").cast("int"),
        F.round(F.col("Delay_Probability_Pct"), 2).cast("float").alias("Delay_Probability_Pct"),
        F.round(F.col("Avg_Delay_Minutes"), 2).cast("float").alias("Avg_Delay_Minutes")
    ).orderBy("Dep_Hour").na.fill(0)
    export_table_csv(t4, "agg_hourly_delays")

    # -------------------------------------------------------------------------
    # Table 5: agg_route_traffic (Q7)
    # -------------------------------------------------------------------------
    print("Computing Table 5: agg_route_traffic...")
    t5 = df_base.groupBy("Origin", "Dest").agg(
        F.first("Route").alias("Route_Name"),
        F.count("*").alias("Flight_Count"),
        F.avg(F.when(F.col("is_operated"), F.col("AirTime"))).alias("Avg_AirTime_Min"),
        F.avg("Distance").alias("Distance_Miles"),
        F.sum(F.when(F.col("is_operated") & (F.col("ArrDel15") == 1), 1).otherwise(0)).alias("Delayed_Flights"),
        F.sum(F.when(F.col("is_operated"), 1).otherwise(0)).alias("Operated_Flights")
    ).withColumn(
        "Route_Delay_Rate_Pct",
        F.when(F.col("Operated_Flights") > 0, (F.col("Delayed_Flights") / F.col("Operated_Flights")) * 100.0).otherwise(0.0)
    ).select(
        F.col("Origin").cast("string"),
        F.col("Dest").cast("string"),
        F.col("Route_Name").cast("string"),
        F.col("Flight_Count").cast("int"),
        F.round(F.coalesce(F.col("Avg_AirTime_Min"), F.lit(0)), 2).cast("float").alias("Avg_AirTime_Min"),
        F.round(F.coalesce(F.col("Distance_Miles"), F.lit(0)), 0).cast("int").alias("Distance_Miles"),
        F.round(F.col("Route_Delay_Rate_Pct"), 2).cast("float").alias("Route_Delay_Rate_Pct")
    ).orderBy(F.col("Flight_Count").desc()).na.fill(0)
    export_table_csv(t5, "agg_route_traffic")

    # -------------------------------------------------------------------------
    # Table 6: agg_calendar_delays (Q5, Q8)
    # -------------------------------------------------------------------------
    print("Computing Table 6: agg_calendar_delays...")
    t6 = df_base.groupBy("Year", "Month", "DayOfWeek").agg(
        F.count("*").alias("Total_Flights"),
        F.sum(F.when(F.col("is_operated") & (F.col("ArrDel15") == 1), 1).otherwise(0)).alias("Delayed_Flights"),
        F.sum(F.when(F.col("Cancelled") == 1, 1).otherwise(0)).alias("Cancelled_Flights"),
        F.sum(F.when(F.col("is_operated"), 1).otherwise(0)).alias("Operated_Flights"),
        F.avg(F.when(F.col("is_operated"), F.coalesce(F.col("ArrDelayMinutes"), F.lit(0)))).alias("Avg_Arr_Delay_Min")
    ).withColumn(
        "Delay_Rate_Pct",
        F.when(F.col("Operated_Flights") > 0, (F.col("Delayed_Flights") / F.col("Operated_Flights")) * 100.0).otherwise(0.0)
    ).select(
        F.col("Year").cast("int"),
        F.col("Month").cast("int"),
        F.col("DayOfWeek").cast("int").alias("Day_Of_Week"),
        F.col("Total_Flights").cast("int"),
        F.col("Delayed_Flights").cast("int"),
        F.col("Cancelled_Flights").cast("int"),
        F.round(F.col("Delay_Rate_Pct"), 2).cast("float").alias("Delay_Rate_Pct"),
        F.round(F.col("Avg_Arr_Delay_Min"), 2).cast("float").alias("Avg_Arr_Delay_Min")
    ).orderBy("Year", "Month", "Day_Of_Week").na.fill(0)
    export_table_csv(t6, "agg_calendar_delays")

    # -------------------------------------------------------------------------
    # Table 7: agg_cancellation_reasons (Q6)
    # -------------------------------------------------------------------------
    print("Computing Table 7: agg_cancellation_reasons...")
    t7 = df_base.filter(F.col("Cancelled") == 1).groupBy("Year", "Reporting_Airline", "CancellationCode").agg(
        F.count("*").alias("Cancelled_Flights")
    ).withColumn(
        "Cancellation_Reason",
        F.when(F.col("CancellationCode") == "A", "Carrier")
         .when(F.col("CancellationCode") == "B", "Weather")
         .when(F.col("CancellationCode") == "C", "National Aviation System (NAS)")
         .when(F.col("CancellationCode") == "D", "Security")
         .otherwise("Unknown")
    ).select(
        F.col("Year").cast("int"),
        F.col("Reporting_Airline").cast("string"),
        F.coalesce(F.col("CancellationCode"), F.lit("Unknown")).cast("string").alias("Cancellation_Code"),
        F.col("Cancellation_Reason").cast("string"),
        F.col("Cancelled_Flights").cast("int")
    ).orderBy("Year", "Reporting_Airline", "Cancellation_Code").na.fill(0)
    export_table_csv(t7, "agg_cancellation_reasons")

    elapsed = time.time() - start_time
    print("=" * 70)
    print(f"SUCCESS: All 7 analytical summary tables computed in {elapsed:.2f} seconds!")
    print(f"Output files stored in {OUTPUT_DIR}")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    main()
