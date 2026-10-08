import requests


# ============================================================
# ClickHouse connection
# ============================================================

CLICKHOUSE_URL = "http://localhost:8123"


# ============================================================
# 1. Create database
# ============================================================

create_database = """
CREATE DATABASE IF NOT EXISTS airline_db
"""


# ============================================================
# 2. Create processed flights table
# ============================================================

create_table = """
CREATE TABLE IF NOT EXISTS airline_db.flights_processed
(
    FlightDate Date,

    Year UInt16,
    Quarter UInt8,
    Month UInt8,
    DayofMonth UInt8,
    DayOfWeek UInt8,

    Reporting_Airline String,
    IATA_CODE_Reporting_Airline String,
    Tail_Number Nullable(String),
    Flight_Number_Reporting_Airline Nullable(Int32),

    OriginAirportID Nullable(Int32),
    Origin String,
    OriginCityName String,
    OriginState String,

    DestAirportID Nullable(Int32),
    Dest String,
    DestCityName String,
    DestState String,

    CRSDepTime Nullable(Int32),
    DepTime Nullable(Int32),
    DepDelay Nullable(Float64),
    DepDelayMinutes Nullable(Float64),
    DepDel15 Nullable(UInt8),

    CRSArrTime Nullable(Int32),
    ArrTime Nullable(Int32),
    ArrDelay Nullable(Float64),
    ArrDelayMinutes Nullable(Float64),
    ArrDel15 Nullable(UInt8),

    TaxiOut Nullable(Float64),
    TaxiIn Nullable(Float64),
    AirTime Nullable(Float64),

    Cancelled Nullable(UInt8),
    CancellationCode Nullable(String),
    Diverted Nullable(UInt8),

    CRSElapsedTime Nullable(Float64),
    ActualElapsedTime Nullable(Float64),

    Flights Nullable(Float64),
    Distance Nullable(Float64),
    DistanceGroup Nullable(UInt8),

    Route String,
    DepartureHour Nullable(UInt8),
    IsDelayed UInt8,

    ProcessedAt DateTime
)
ENGINE = MergeTree
ORDER BY (FlightDate, Reporting_Airline, Origin, Dest)
"""


# ============================================================
# Execute database creation
# ============================================================

print("Connecting to ClickHouse...")

response = requests.post(
    CLICKHOUSE_URL,
    params={"query": create_database}
)

response.raise_for_status()

print("Database 'airline_db' is ready.")


# ============================================================
# Execute table creation
# ============================================================

response = requests.post(
    CLICKHOUSE_URL,
    params={"query": create_table}
)

response.raise_for_status()

print("Table 'flights_processed' is ready.")

print("ClickHouse setup completed successfully.")