import glob
import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=[
        'localhost:9092',
        'localhost:9093',
        'localhost:9094'
    ],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_files = glob.glob("data/raw/*.csv")

print(f"Found {len(csv_files)} CSV files")

for file_path in csv_files:
    print(f"Streaming file: {file_path}")

    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        count = 0

        for row in reader:
            key = f"{row['Origin']}_{row['Dest']}"

            producer.send(
                'airline-flights',
                key=key.encode('utf-8'),
                value=row
            )

            count += 1

        print(f"Sent {count} messages from {file_path}")

producer.flush()
producer.close()

print("Finished streaming all CSV files.")