from kafka import KafkaProducer
import json

# Khởi tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # sửa theo config của bạn
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_to_kafka(topic: str, data: dict):
    try:
        producer.send(topic, value=data)
        producer.flush()
    except Exception as e:
        print(f"[KAFKA] Failed to send message: {e}")
