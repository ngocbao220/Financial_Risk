from confluent_kafka import Producer
import json
import os
import logging

# Enable log của librdkafka để thấy rõ kết nối thành công hay không
logging.basicConfig(level=logging.INFO)

def delivery_report(err, msg):
    """Callback gọi khi message được gửi thành công hoặc thất bại"""
    if err is not None:
        print(f"[KAFKA] Failed to deliver message: {err}")
    else:
        print(f"[KAFKA] Sent to {msg.topic()} [partition {msg.partition()}]")

# Lấy bootstrap servers từ env (rất quan trọng để linh hoạt host/container)
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")

# Nếu bạn chạy producer TRỰC TIẾP TRÊN MAC (không qua Docker) → dùng localhost cũng được
# Nhưng an toàn nhất là dùng host.docker.internal
producer = Producer({
    'bootstrap.servers': bootstrap_servers,
    'message.timeout.ms': 10000,        # Timeout gửi message
    'retries': 3,
    'acks': 1,
    # Nếu bạn thấy log quá nhiều thì comment dòng dưới
    'log_level': 6
})

def send_to_kafka(topic: str, data: dict):
    try:
        # Dùng callback để biết chắc message đã được gửi thành công
        producer.produce(
            topic=topic,
            value=json.dumps(data).encode('utf-8'),
            callback=delivery_report
        )
        # Quan trọng: poll để trigger callback
        producer.poll(0)
    except Exception as e:
        print(f"[KAFKA] Exception when sending message: {e}")

# Gợi ý: thêm hàm flush khi tắt chương trình
def shutdown():
    print("[KAFKA] Flushing remaining messages...")
    producer.flush(timeout=10)  # chờ tối đa 10s