import os
import time
from collections import Counter
from datetime import date, datetime
from decimal import Decimal

import psycopg
from psycopg import sql
from kafka import KafkaConsumer, TopicPartition

from source import read_sales


def source_column(name):
    explicit = {
        "id": ("f", "source_id"),
        "event_id": ("f", "source_row_id"),
        "source_file": ("f", "source_file"),
        "source_row": ("f", "source_row"),
        "sale_customer_id": ("c", "source_customer_id"),
        "sale_seller_id": ("s", "source_seller_id"),
        "sale_product_id": ("p", "source_product_id"),
        "sale_date": ("d", "full_date"),
        "sale_quantity": ("f", "sale_quantity"),
        "sale_total_price": ("f", "sale_total_price"),
        "pet_category": ("p", "pet_category"),
    }
    if name in explicit:
        return explicit[name]
    for prefix, alias in (("customer_", "c"), ("seller_", "s"),
                          ("product_", "p"), ("store_", "st"), ("supplier_", "sp")):
        if name.startswith(prefix):
            return alias, name.removeprefix(prefix) if alias in ("c", "s") else name
    raise ValueError(f"Unknown source column: {name}")


def matches(expected, actual):
    if expected is None or actual is None:
        return expected is actual
    if isinstance(actual, date):
        return datetime.strptime(expected, "%m/%d/%Y").date() == actual
    if isinstance(actual, (int, Decimal)):
        return Decimal(str(expected)) == Decimal(actual)
    return expected == actual


def wait_for_offsets(timeout):
    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        group_id="flink-star-lab3", enable_auto_commit=False,
    )
    try:
        topic = os.getenv("KAFKA_TOPIC", "sales")
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"Missing Kafka topic: {topic}")
        ends = consumer.end_offsets([TopicPartition(topic, p) for p in partitions])
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if all((consumer.committed(partition) or 0) >= end for partition, end in ends.items()):
                print(f"KAFKA OK: {sum(ends.values())} messages covered by committed Flink checkpoints")
                return
            time.sleep(2)
        raise TimeoutError("Flink has not checkpointed all Kafka offsets; inspect Flink UI and logs")
    finally:
        consumer.close()


def main():
    rows = list(read_sales())
    expected = {row["event_id"]: row for row in rows}
    files = Counter(row["source_file"] for row in rows)
    if len(expected) != 10000 or len(files) != 10 or set(files.values()) != {1000}:
        raise RuntimeError(f"Expected 10 CSV files with 1000 distinct events each, got {files}")
    wait_for_offsets(int(os.getenv("VERIFY_TIMEOUT", "180")))
    with psycopg.connect(
        host=os.getenv("PG_HOST", "postgres"), dbname=os.getenv("PG_DATABASE", "lab_db"),
        user=os.getenv("PG_USER", "postgres"), password=os.getenv("PG_PASSWORD", "postgres"),
    ) as connection:
        columns = list(rows[0])
        projections = []
        for name in columns:
            alias, column = source_column(name)
            projections.append(sql.SQL("{}.{}").format(sql.Identifier(alias), sql.Identifier(column)))
        query = sql.SQL("""
            SELECT {} FROM fact_sales f
            JOIN dim_customer c USING (customer_key)
            JOIN dim_seller s USING (seller_key)
            JOIN dim_product p USING (product_key)
            JOIN dim_store st USING (store_key)
            JOIN dim_supplier sp USING (supplier_key)
            JOIN dim_date d USING (date_key)
        """).format(sql.SQL(", ").join(projections))
        seen = set()
        for values in connection.execute(query):
            actual = dict(zip(columns, values))
            event = actual["event_id"]
            if event not in expected or event in seen:
                raise RuntimeError(f"Unexpected or duplicate persisted event: {event}")
            for name in columns:
                if not matches(expected[event][name], actual[name]):
                    raise RuntimeError(f"Mismatch {event}.{name}: {expected[event][name]!r} != {actual[name]!r}")
            seen.add(event)
        if seen != set(expected):
            raise RuntimeError(f"Missing events: {len(set(expected) - seen)}")
        for entity in ("customer", "seller", "product", "store", "supplier", "date"):
            if entity in ("customer", "seller", "product"):
                count = len(rows)
            elif entity == "date":
                count = len({row["sale_date"] for row in rows})
            else:
                fields = [name for name in columns if name.startswith(entity + "_")]
                count = len({tuple(row[name] for name in fields) for row in rows})
            actual_count = connection.execute(
                sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier("dim_" + entity))
            ).fetchone()[0]
            if actual_count != count:
                raise RuntimeError(f"dim_{entity}: expected {count} rows, got {actual_count}")
        sales, units, revenue = connection.execute(
            "SELECT count(*), sum(sale_quantity), sum(sale_total_price) FROM fact_sales"
        ).fetchone()
        print(f"POSTGRES OK: {sales} sales, {units} units, revenue {revenue}")
        print(f"SOURCE OK: all {len(columns)} fields of every CSV row match the joined star schema")
        print("DIMENSIONS OK: counts match source, no replay duplicates")


if __name__ == "__main__":
    main()
