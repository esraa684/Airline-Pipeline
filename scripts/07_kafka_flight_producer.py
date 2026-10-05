#!/usr/bin/env python3
"""
Flight & Airport Big Data Analytics Platform
Script 07: Real-Time Kafka Flight Event Producer
Owner: Member 1 / Streaming Integration

Publishes simulated real-time flight departure and arrival events into
Apache Kafka topic 'flight-events' to demonstrate streaming ingestion in Kafka-UI.
"""

import sys
import time
import json
import random
from datetime import datetime

# Sample airlines and airports for realistic simulation
AIRLINES = ["AA", "DL", "UA", "WN", "B6", "AS", "NK", "F9"]
AIRLINE_NAMES = {
    "AA": "American Airlines", "DL": "Delta Air Lines", "UA": "United Airlines",
    "WN": "Southwest Airlines", "B6": "JetBlue Airways", "AS": "Alaska Airlines",
    "NK": "Spirit Airlines", "F9": "Frontier Airlines"
}
AIRPORTS = ["JFK", "LAX", "ORD", "DFW", "ATL", "DEN", "SFO", "SEA", "MCO", "BOS", "MIA", "CLT"]
DELAY_CAUSES = ["None", "CarrierDelay", "WeatherDelay", "NASDelay", "LateAircraftDelay"]

def generate_flight_event():
    """Generates a realistic single flight event dictionary."""
    origin, dest = random.sample(AIRPORTS, 2)
    airline = random.choice(AIRLINES)
    flight_num = random.randint(100, 2999)
    scheduled_hour = random.randint(5, 23)
    scheduled_min = random.choice([0, 15, 30, 45])
    
    # 80% on-time, 20% delayed probability distribution
    is_delayed = 1 if random.random() < 0.22 else 0
    dep_delay = random.randint(16, 120) if is_delayed else random.randint(-5, 10)
    arr_delay = dep_delay + random.randint(-10, 15)
    cause = random.choice(DELAY_CAUSES[1:]) if is_delayed else "None"

    return {
        "event_timestamp": datetime.utcnow().isoformat() + "Z",
        "flight_id": f"{airline}{flight_num}",
        "reporting_airline": airline,
        "airline_name": AIRLINE_NAMES[airline],
        "origin": origin,
        "dest": dest,
        "crs_dep_hour": scheduled_hour,
        "crs_dep_time": f"{scheduled_hour:02d}:{scheduled_min:02d}",
        "dep_delay_minutes": max(0, dep_delay),
        "arr_delay_minutes": max(0, arr_delay),
        "arr_del15": 1 if arr_delay >= 15 else 0,
        "delay_cause": cause,
        "status": "Delayed" if arr_delay >= 15 else "On-Time"
    }

def main():
    topic = "flight-events"
    bootstrap_server = "localhost:9094"  # Default external port on Windows host

    print("==================================================")
    print("      Apache Kafka Live Flight Event Producer     ")
    print(f" Target Topic:     {topic}")
    print(f" Bootstrap Server: {bootstrap_server}")
    print("==================================================")

    # Try importing kafka-python, fallback to stdout JSON stream for pipe
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(" Connected to Kafka Broker successfully! Publishing live stream...")
        count = 0
        while True:
            event = generate_flight_event()
            producer.send(topic, value=event)
            count += 1
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Emitted #{count}: {event['flight_id']} ({event['origin']} -> {event['dest']}) | Status: {event['status']} ({event['arr_delay_minutes']} min)")
            time.sleep(random.uniform(0.5, 1.5))
    except (ImportError, Exception) as e:
        print(f"[*] Note: kafka-python not directly attached or connecting ({e}).")
        print("[*] Generating pure JSON event stream to STDOUT (can pipe to kafka-console-producer):")
        print("--------------------------------------------------")
        count = 0
        try:
            while count < 50:
                event = generate_flight_event()
                count += 1
                print(json.dumps(event))
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\nStopped.")

if __name__ == "__main__":
    main()
