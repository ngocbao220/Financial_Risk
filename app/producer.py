import csv, json, time, os
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "risk_signals"
CSV_PATH = "/data/aggregated_risk.csv"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def row_to_event(row):
    # Chuyển đổi định dạng hàng CSV sang JSON
    return {
        "ticker": row["ticker"],
        "date": row["date"],
        "accounting": float(row["accounting"] or 0),
        "misstatement": float(row["misstatement"] or 0),
        "events": float(row["events"] or 0),
        "risk": float(row["risk"] or 0),
        "risk_ind_adjs": float(row["risk_ind_adjs"] or 0)
    }

def produce():
    with open(CSV_PATH, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            event = row_to_event(row)
            producer.send(TOPIC, event)
            print(f"Sent: {event}")
            time.sleep(0.05)  # giả lập realtime stream
    producer.flush()

if __name__ == "__main__":
    produce()
