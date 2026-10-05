import os
import urllib.request

CLICKHOUSE_HTTP = "http://localhost:8123/?user=default&password=clickhouse&database=flight_analytics&query="
CSV_DIR = r"C:\Users\Abdelwadoud\Documents\BigData-NTI-Files\bigdata-lab\bigdata-lab\spark\apps\output"

TABLES = [
    "agg_airline_performance",
    "agg_airport_performance",
    "agg_calendar_delays",
    "agg_cancellation_reasons",
    "agg_delay_causes_monthly",
    "agg_hourly_delays",
    "agg_route_traffic"
]

def load_tables():
    print("=" * 60)
    print("Loading Spark-Generated CSV Aggregates into ClickHouse...")
    print("=" * 60)

    for table in TABLES:
        csv_file = os.path.join(CSV_DIR, f"{table}.csv")
        if not os.path.exists(csv_file):
            print(f"Warning: {csv_file} not found. Skipping.")
            continue

        # Truncate existing table before loading
        trunc_url = CLICKHOUSE_HTTP + f"TRUNCATE+TABLE+{table}"
        req_trunc = urllib.request.Request(trunc_url, method='POST')
        urllib.request.urlopen(req_trunc)

        # Ingest CSV with names
        insert_url = CLICKHOUSE_HTTP + f"INSERT+INTO+{table}+FORMAT+CSVWithNames"
        with open(csv_file, 'rb') as f:
            csv_data = f.read()

        req_insert = urllib.request.Request(insert_url, data=csv_data, method='POST')
        try:
            with urllib.request.urlopen(req_insert) as resp:
                # Query count
                count_url = CLICKHOUSE_HTTP + f"SELECT+count(*)+FROM+{table}"
                with urllib.request.urlopen(count_url) as c_resp:
                    rows = c_resp.read().decode('utf-8').strip()
                print(f" Loaded {table} -> {rows} rows in ClickHouse")
        except Exception as e:
            print(f" Error loading {table}: {e}")

    print("=" * 60)
    print("All 7 analytical tables successfully populated in ClickHouse!")
    print("=" * 60)

if __name__ == "__main__":
    load_tables()
