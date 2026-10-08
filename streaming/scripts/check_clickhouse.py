import requests


# ============================================================
# ClickHouse connection
# ============================================================

CLICKHOUSE_URL = "http://localhost:8123"


def run_query(query):
    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query}
    )

    response.raise_for_status()

    return response.text.strip()


# ============================================================
# Check connection
# ============================================================

print("Testing ClickHouse connection...")

result = run_query("SELECT 1")

print(f"Connection test: {result}")


# ============================================================
# Count flights
# ============================================================

query = """
SELECT count()
FROM airline_db.flights_processed
"""

count = run_query(query)

print(f"Flights in ClickHouse: {count}")


# ============================================================
# Show sample records
# ============================================================

query = """
SELECT
    FlightDate,
    Reporting_Airline,
    Origin,
    Dest,
    Route,
    ArrDelay,
    IsDelayed,
    ProcessedAt
FROM airline_db.flights_processed
LIMIT 5
FORMAT TabSeparated
"""

sample = run_query(query)

print("\nSample records:")
print(sample)