from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    concat_ws,
    when,
    floor,
    to_date,
    expr,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# ============================================================
# 1. Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("AirlineKafkaStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Kafka JSON Schema
#
# Keep everything as StringType initially.
#
# Why?
# The CSV data contains empty strings in numeric fields.
# We safely convert them later using try_cast().
# ============================================================

schema = StructType([

    # -------------------------
    # Date / Time
    # -------------------------
    StructField("Year", StringType(), True),
    StructField("Quarter", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("DayofMonth", StringType(), True),
    StructField("DayOfWeek", StringType(), True),
    StructField("FlightDate", StringType(), True),

    # -------------------------
    # Airline / Flight
    # -------------------------
    StructField("Reporting_Airline", StringType(), True),
    StructField("DOT_ID_Reporting_Airline", StringType(), True),
    StructField("IATA_CODE_Reporting_Airline", StringType(), True),
    StructField("Tail_Number", StringType(), True),
    StructField("Flight_Number_Reporting_Airline", StringType(), True),

    # -------------------------
    # Origin
    # -------------------------
    StructField("OriginAirportID", StringType(), True),
    StructField("OriginAirportSeqID", StringType(), True),
    StructField("OriginCityMarketID", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("OriginState", StringType(), True),
    StructField("OriginStateFips", StringType(), True),
    StructField("OriginStateName", StringType(), True),
    StructField("OriginWac", StringType(), True),

    # -------------------------
    # Destination
    # -------------------------
    StructField("DestAirportID", StringType(), True),
    StructField("DestAirportSeqID", StringType(), True),
    StructField("DestCityMarketID", StringType(), True),
    StructField("Dest", StringType(), True),
    StructField("DestCityName", StringType(), True),
    StructField("DestState", StringType(), True),
    StructField("DestStateFips", StringType(), True),
    StructField("DestStateName", StringType(), True),
    StructField("DestWac", StringType(), True),

    # -------------------------
    # Departure
    # -------------------------
    StructField("CRSDepTime", StringType(), True),
    StructField("DepTime", StringType(), True),
    StructField("DepDelay", StringType(), True),
    StructField("DepDelayMinutes", StringType(), True),
    StructField("DepDel15", StringType(), True),
    StructField("DepartureDelayGroups", StringType(), True),
    StructField("DepTimeBlk", StringType(), True),

    # -------------------------
    # Taxi / Wheels
    # -------------------------
    StructField("TaxiOut", StringType(), True),
    StructField("WheelsOff", StringType(), True),
    StructField("WheelsOn", StringType(), True),
    StructField("TaxiIn", StringType(), True),

    # -------------------------
    # Arrival
    # -------------------------
    StructField("CRSArrTime", StringType(), True),
    StructField("ArrTime", StringType(), True),
    StructField("ArrDelay", StringType(), True),
    StructField("ArrDelayMinutes", StringType(), True),
    StructField("ArrDel15", StringType(), True),
    StructField("ArrivalDelayGroups", StringType(), True),
    StructField("ArrTimeBlk", StringType(), True),

    # -------------------------
    # Flight status
    # -------------------------
    StructField("Cancelled", StringType(), True),
    StructField("CancellationCode", StringType(), True),
    StructField("Diverted", StringType(), True),

    # -------------------------
    # Duration
    # -------------------------
    StructField("CRSElapsedTime", StringType(), True),
    StructField("ActualElapsedTime", StringType(), True),
    StructField("AirTime", StringType(), True),

    # -------------------------
    # Distance
    # -------------------------
    StructField("Flights", StringType(), True),
    StructField("Distance", StringType(), True),
    StructField("DistanceGroup", StringType(), True)
])


# ============================================================
# 3. Read Kafka
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka-broker-1:19092,"
        "kafka-broker-2:19092,"
        "kafka-broker-3:19092"
    )
    .option("subscribe", "airline-flights")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# ============================================================
# 4. Kafka Binary
#    ->
#    String
#    ->
#    JSON
#    ->
#    Spark columns
# ============================================================

parsed_stream = (
    raw_stream

    .selectExpr(
        "CAST(value AS STRING) AS json_value"
    )

    .select(
        from_json(
            col("json_value"),
            schema
        ).alias("data")
    )

    .select("data.*")
)


# ============================================================
# 5. Safe Type Conversion
#
# IMPORTANT:
#
# try_cast("123" AS DOUBLE) -> 123
# try_cast("" AS DOUBLE)    -> NULL
# try_cast("abc" AS DOUBLE) -> NULL
#
# This prevents one bad/missing value from killing
# the entire streaming query.
# ============================================================

typed_stream = (
    parsed_stream

    # -------------------------
    # Date / Time
    # -------------------------
    .withColumn(
        "Year",
        expr("try_cast(Year AS SMALLINT)")
    )
    .withColumn(
        "Quarter",
        expr("try_cast(Quarter AS TINYINT)")
    )
    .withColumn(
        "Month",
        expr("try_cast(Month AS TINYINT)")
    )
    .withColumn(
        "DayofMonth",
        expr("try_cast(DayofMonth AS TINYINT)")
    )
    .withColumn(
        "DayOfWeek",
        expr("try_cast(DayOfWeek AS TINYINT)")
    )
    .withColumn(
        "FlightDate",
        to_date(col("FlightDate"))
    )

    # -------------------------
    # Airline / Flight
    # -------------------------
    .withColumn(
        "DOT_ID_Reporting_Airline",
        expr("try_cast(DOT_ID_Reporting_Airline AS INT)")
    )
    .withColumn(
        "Flight_Number_Reporting_Airline",
        expr("try_cast(Flight_Number_Reporting_Airline AS INT)")
    )

    # -------------------------
    # Origin
    # -------------------------
    .withColumn(
        "OriginAirportID",
        expr("try_cast(OriginAirportID AS INT)")
    )
    .withColumn(
        "OriginAirportSeqID",
        expr("try_cast(OriginAirportSeqID AS INT)")
    )
    .withColumn(
        "OriginCityMarketID",
        expr("try_cast(OriginCityMarketID AS INT)")
    )
    .withColumn(
        "OriginStateFips",
        expr("try_cast(OriginStateFips AS INT)")
    )
    .withColumn(
        "OriginWac",
        expr("try_cast(OriginWac AS INT)")
    )

    # -------------------------
    # Destination
    # -------------------------
    .withColumn(
        "DestAirportID",
        expr("try_cast(DestAirportID AS INT)")
    )
    .withColumn(
        "DestAirportSeqID",
        expr("try_cast(DestAirportSeqID AS INT)")
    )
    .withColumn(
        "DestCityMarketID",
        expr("try_cast(DestCityMarketID AS INT)")
    )
    .withColumn(
        "DestStateFips",
        expr("try_cast(DestStateFips AS INT)")
    )
    .withColumn(
        "DestWac",
        expr("try_cast(DestWac AS INT)")
    )

    # -------------------------
    # Departure
    # -------------------------
    .withColumn(
        "CRSDepTime",
        expr("try_cast(CRSDepTime AS INT)")
    )
    .withColumn(
        "DepTime",
        expr("try_cast(DepTime AS INT)")
    )
    .withColumn(
        "DepDelay",
        expr("try_cast(DepDelay AS DOUBLE)")
    )
    .withColumn(
        "DepDelayMinutes",
        expr("try_cast(DepDelayMinutes AS DOUBLE)")
    )
    .withColumn(
        "DepDel15",
        expr("try_cast(DepDel15 AS TINYINT)")
    )
    .withColumn(
        "DepartureDelayGroups",
        expr("try_cast(DepartureDelayGroups AS INT)")
    )

    # -------------------------
    # Taxi / Wheels
    # -------------------------
    .withColumn(
        "TaxiOut",
        expr("try_cast(TaxiOut AS DOUBLE)")
    )
    .withColumn(
        "WheelsOff",
        expr("try_cast(WheelsOff AS INT)")
    )
    .withColumn(
        "WheelsOn",
        expr("try_cast(WheelsOn AS INT)")
    )
    .withColumn(
        "TaxiIn",
        expr("try_cast(TaxiIn AS DOUBLE)")
    )

    # -------------------------
    # Arrival
    # -------------------------
    .withColumn(
        "CRSArrTime",
        expr("try_cast(CRSArrTime AS INT)")
    )
    .withColumn(
        "ArrTime",
        expr("try_cast(ArrTime AS INT)")
    )
    .withColumn(
        "ArrDelay",
        expr("try_cast(ArrDelay AS DOUBLE)")
    )
    .withColumn(
        "ArrDelayMinutes",
        expr("try_cast(ArrDelayMinutes AS DOUBLE)")
    )
    .withColumn(
        "ArrDel15",
        expr("try_cast(ArrDel15 AS TINYINT)")
    )
    .withColumn(
        "ArrivalDelayGroups",
        expr("try_cast(ArrivalDelayGroups AS INT)")
    )

    # -------------------------
    # Flight status
    # -------------------------
    .withColumn(
        "Cancelled",
        expr("try_cast(try_cast(Cancelled AS DOUBLE) AS TINYINT)")
    )
    .withColumn(
        "Diverted",
        expr("try_cast(try_cast(Diverted AS DOUBLE) AS TINYINT)")
    )

    # -------------------------
    # Duration
    # -------------------------
    .withColumn(
        "CRSElapsedTime",
        expr("try_cast(CRSElapsedTime AS DOUBLE)")
    )
    .withColumn(
        "ActualElapsedTime",
        expr("try_cast(ActualElapsedTime AS DOUBLE)")
    )
    .withColumn(
        "AirTime",
        expr("try_cast(AirTime AS DOUBLE)")
    )

    # -------------------------
    # Distance
    # -------------------------
    .withColumn(
        "Flights",
        expr("try_cast(Flights AS DOUBLE)")
    )
    .withColumn(
        "Distance",
        expr("try_cast(Distance AS DOUBLE)")
    )
    .withColumn(
        "DistanceGroup",
        expr("try_cast(DistanceGroup AS INT)")
    )
)


# ============================================================
# 6. Feature Engineering
# ============================================================

processed_stream = (
    typed_stream

    # -------------------------
    # Route
    #
    # Example:
    # CMH + DCA
    # ->
    # CMH_DCA
    # -------------------------
    .withColumn(
        "Route",
        concat_ws(
            "_",
            col("Origin"),
            col("Dest")
        )
    )

    # -------------------------
    # Departure hour
    #
    # 1224 -> 12
    # 530  -> 5
    #
    # NULL stays NULL
    # -------------------------
    .withColumn(
        "DepartureHour",
        floor(
            col("CRSDepTime") / 100
        ).cast("int")
    )

    # -------------------------
    # Delay classification
    #
    # ArrDelay > 15 -> delayed
    # ArrDelay <= 15 -> not delayed
    #
    # Missing ArrDelay -> 0
    # -------------------------
    .withColumn(
        "IsDelayed",
        when(
            col("ArrDelay") > 15,
            1
        ).otherwise(0)
    )

    .withColumn(
        "ProcessedAt",
        current_timestamp()
    )
)


# ============================================================
# 7. Final downstream schema
#
# This is the schema that can be mapped directly to
# ClickHouse later.
# ============================================================

final_stream = processed_stream.select(

    # -------------------------
    # Date
    # -------------------------
    "FlightDate",
    "Year",
    "Quarter",
    "Month",
    "DayofMonth",
    "DayOfWeek",

    # -------------------------
    # Airline / Flight
    # -------------------------
    "Reporting_Airline",
    "IATA_CODE_Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",

    # -------------------------
    # Origin
    # -------------------------
    "OriginAirportID",
    "Origin",
    "OriginCityName",
    "OriginState",

    # -------------------------
    # Destination
    # -------------------------
    "DestAirportID",
    "Dest",
    "DestCityName",
    "DestState",

    # -------------------------
    # Departure
    # -------------------------
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",

    # -------------------------
    # Arrival
    # -------------------------
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",

    # -------------------------
    # Operations
    # -------------------------
    "TaxiOut",
    "TaxiIn",
    "AirTime",

    # -------------------------
    # Status
    # -------------------------
    "Cancelled",
    "CancellationCode",
    "Diverted",

    # -------------------------
    # Duration
    # -------------------------
    "CRSElapsedTime",
    "ActualElapsedTime",

    # -------------------------
    # Distance
    # -------------------------
    "Flights",
    "Distance",
    "DistanceGroup",

    # -------------------------
    # Engineered features
    # -------------------------
    # -------------------------

    "Route",
    "DepartureHour",
    "IsDelayed",
    "ProcessedAt"

)



# ============================================================
# 8. Write each Spark micro-batch to ClickHouse
# ============================================================
def write_to_clickhouse(batch_df, batch_id):
    print(f"Writing batch {batch_id} to ClickHouse...")

    (
        batch_df.write
        .format("jdbc")
        .option(
            "url",
            "jdbc:clickhouse://clickhouse:8123/airline_db?compress=0"
        )
        .option("dbtable", "flights_processed")
        .option(
            "driver",
            "com.clickhouse.jdbc.ClickHouseDriver"
        )
        .option("user", "default")
        .option("password", "")
        .option("batchsize", "5000")
        .option("isolationLevel", "NONE")
        .mode("append")
        .save()
    )

    print(f"Batch {batch_id} written to ClickHouse.")


query = (
    final_stream
    .writeStream
    .foreachBatch(write_to_clickhouse)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "hdfs://namenode:9000/checkpoints/airline-flights"
    )
    .start()
)

query.awaitTermination()