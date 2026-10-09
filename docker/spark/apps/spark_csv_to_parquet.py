import sys
import time
from pyspark.sql import SparkSession, functions as F

RAW_PATH = "hdfs://namenode:9000/project/flights/raw/*.csv"
PARQUET_OUT = "hdfs://namenode:9000/project/flights/parquet"

COLS = {
    "Year": "int",
    "Month": "int",
    "DayofMonth": "int",
    "DayOfWeek": "int",
    "FlightDate": "date",
    "Reporting_Airline": "string",
    "Tail_Number": "string",
    "Flight_Number_Reporting_Airline": "string",
    "Origin": "string",
    "OriginCityName": "string",
    "OriginState": "string",
    "Dest": "string",
    "DestCityName": "string",
    "DestState": "string",
    "CRSDepTime": "double",
    "DepTime": "double",
    "DepDelay": "double",
    "DepDelayMinutes": "double",
    "DepDel15": "double",
    "CRSArrTime": "double",
    "ArrTime": "double",
    "ArrDelay": "double",
    "ArrDelayMinutes": "double",
    "ArrDel15": "double",
    "Cancelled": "double",
    "CancellationCode": "string",
    "Diverted": "double",
    "CRSElapsedTime": "double",
    "ActualElapsedTime": "double",
    "AirTime": "double",
    "Distance": "double",
    "CarrierDelay": "double",
    "WeatherDelay": "double",
    "NASDelay": "double",
    "SecurityDelay": "double",
    "LateAircraftDelay": "double",
}

def main():
    print(f"Initializing Spark Session for CSV -> Parquet Conversion...")
    start_t = time.time()
    
    spark = (
        SparkSession.builder
        .appName("FlightAnalytics_CSV_to_Parquet")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    print(f"Reading raw flight CSVs from {RAW_PATH}...")
    df = spark.read.option("header", "true").csv(RAW_PATH)
    
    # Select known columns by name and cast (handles trailing commas cleanly)
    selected_cols = []
    for c, t in COLS.items():
        if c in df.columns:
            selected_cols.append(F.col(c).cast(t).alias(c))
        else:
            print(f"Warning: column {c} not found in input; filling null.")
            selected_cols.append(F.lit(None).cast(t).alias(c))

    cleaned_df = df.select(selected_cols)
    
    # Add derived columns
    cleaned_df = (
        cleaned_df
        .withColumn("Dep_Hour", (F.floor(F.col("CRSDepTime") / 100) % 24).cast("int"))
        .withColumn("Route", F.concat_ws("-", F.col("Origin"), F.col("Dest")))
    )

    print(f"Writing cleaned, partitioned Parquet to {PARQUET_OUT}...")
    cleaned_df.write.mode("overwrite").partitionBy("Year").parquet(PARQUET_OUT)
    
    elapsed = time.time() - start_t
    print(f"SUCCESS: Parquet conversion completed in {elapsed:.2f} seconds.")
    
    # Validation
    parquet_df = spark.read.parquet(PARQUET_OUT)
    row_count = parquet_df.count()
    print(f"Validated Parquet row count: {row_count:,} records.")
    parquet_df.printSchema()
    
    spark.stop()

if __name__ == "__main__":
    main()
