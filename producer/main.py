import json
import os
import time

from kafka import KafkaProducer

from source import read_sales


def main():
    interval = float(os.getenv("SEND_INTERVAL", "0.005"))
    if interval < 0:
        raise ValueError("SEND_INTERVAL must be nonnegative")
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )
    pending = []
    count = 0
    try:
        for row in read_sales():
            pending.append(producer.send(os.getenv("KAFKA_TOPIC", "sales"),
                                         key=row["event_id"], value=row))
            count += 1
            if interval:
                time.sleep(interval)
            if len(pending) == 500:
                for future in pending:
                    future.get(timeout=60)
                pending.clear()
                print(f"SENT {count} sales", flush=True)
        for future in pending:
            future.get(timeout=60)
        producer.flush(timeout=60)
        print(f"DONE: {count} JSON messages acknowledged by Kafka", flush=True)
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
