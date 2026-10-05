import urllib.request
import urllib.parse

CLICKHOUSE_URL = "http://localhost:8123/?user=default&password=clickhouse&database=flight_analytics"

DDL_QUERIES = [
    """
    CREATE TABLE IF NOT EXISTS agg_airline_performance (
        Year UInt16,
        Reporting_Airline String,
        Airline_Name String,
        Total_Flights UInt32,
        OnTime_Flights UInt32,
        Delayed_Flights UInt32,
        Delay_Rate_Pct Float32,
        Avg_Dep_Delay_Min Float32,
        Avg_Arr_Delay_Min Float32,
        Cancelled_Flights UInt32,
        Cancellation_Rate_Pct Float32
    ) ENGINE = MergeTree()
    ORDER BY (Year, Reporting_Airline)
    """,
    """
    CREATE TABLE IF NOT EXISTS agg_airport_performance (
        Airport_Code String,
        Airport_City String,
        Airport_State String,
        Total_Departures UInt32,
        Total_Arrivals UInt32,
        Dep_Delay_Rate_Pct Float32,
        Arr_Delay_Rate_Pct Float32,
        Avg_Dep_Delay_Min Float32,
        Cancelled_Departures UInt32
    ) ENGINE = MergeTree()
    ORDER BY Airport_Code
    """,
    """
    CREATE TABLE IF NOT EXISTS agg_delay_causes_monthly (
        Year UInt16,
        Month UInt8,
        Total_Delay_Minutes UInt64,
        Carrier_Delay_Min UInt64,
        Weather_Delay_Min UInt64,
        NAS_Delay_Min UInt64,
        Security_Delay_Min UInt64,
        Late_Aircraft_Delay_Min UInt64,
        Carrier_Pct Float32,
        Weather_Pct Float32,
        NAS_Pct Float32,
        Security_Pct Float32,
        Late_Aircraft_Pct Float32
    ) ENGINE = MergeTree()
    ORDER BY (Year, Month)
    """,
    """
    CREATE TABLE IF NOT EXISTS agg_hourly_delays (
        Dep_Hour UInt8,
        Total_Flights UInt32,
        Delayed_Flights UInt32,
        Delay_Probability_Pct Float32,
        Avg_Delay_Minutes Float32
    ) ENGINE = MergeTree()
    ORDER BY Dep_Hour
    """,
    """
    CREATE TABLE IF NOT EXISTS agg_route_traffic (
        Origin String,
        Dest String,
        Route_Name String,
        Flight_Count UInt32,
        Avg_AirTime_Min Float32,
        Distance_Miles UInt32,
        Route_Delay_Rate_Pct Float32
    ) ENGINE = MergeTree()
    ORDER BY (Origin, Dest)
    """,
    """
    CREATE TABLE IF NOT EXISTS agg_calendar_delays (
        Year UInt16,
        Month UInt8,
        Day_Of_Week UInt8,
        Total_Flights UInt32,
        Delayed_Flights UInt32,
        Cancelled_Flights UInt32,
        Delay_Rate_Pct Float32,
        Avg_Arr_Delay_Min Float32
    ) ENGINE = MergeTree()
    ORDER BY (Year, Month, Day_Of_Week)
    """,
    """
    CREATE TABLE IF NOT EXISTS agg_cancellation_reasons (
        Year UInt16,
        Reporting_Airline String,
        Cancellation_Code String,
        Cancellation_Reason String,
        Cancelled_Flights UInt32
    ) ENGINE = MergeTree()
    ORDER BY (Year, Reporting_Airline, Cancellation_Code)
    """,
    """
    CREATE TABLE IF NOT EXISTS ml_delay_predictions (
        FlightDate Date,
        Reporting_Airline String,
        Origin String,
        Dest String,
        Dep_Hour UInt8,
        Actual_Delayed UInt8,
        Predicted_Delayed UInt8,
        Delay_Probability Float32,
        Risk_Category String
    ) ENGINE = MergeTree()
    ORDER BY (FlightDate, Reporting_Airline)
    """
]

def execute_ddl():
    print("Creating ClickHouse tables in database 'flight_analytics'...")
    for idx, sql in enumerate(DDL_QUERIES, 1):
        table_name = sql.strip().split()[5].split('(')[0]
        req = urllib.request.Request(CLICKHOUSE_URL, data=sql.strip().encode('utf-8'), method='POST')
        try:
            with urllib.request.urlopen(req) as resp:
                print(f"[{idx}/8] Created/Verified: {table_name}")
        except Exception as e:
            print(f"[{idx}/8] Error creating {table_name}: {e}")

    # List all tables in flight_analytics
    req = urllib.request.Request(CLICKHOUSE_URL + "&query=SHOW+TABLES", method='GET')
    with urllib.request.urlopen(req) as resp:
        tables = resp.read().decode('utf-8').strip().split('\n')
        print(f"\nTotal tables in flight_analytics: {len(tables)}")
        for t in tables:
            print(f" - {t}")

if __name__ == "__main__":
    execute_ddl()
