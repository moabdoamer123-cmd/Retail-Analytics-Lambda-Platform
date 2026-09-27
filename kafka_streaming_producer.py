import argparse
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from retail_common import clean, get_category, get_payment, load_raw, split_cutoff

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "ecommerce-orders"


def on_error(exc):
    print(f"❌ Delivery failed: {exc}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="online_retail_II.csv")
    ap.add_argument("--delay", type=float, default=0.2, help="seconds between messages (0 = as fast as possible)")
    ap.add_argument("--limit", type=int, default=None, help="max number of orders to send (for a demo)")
    args = ap.parse_args()

    print("⏳ Loading dataset...")
    cleaned = clean(load_raw(args.file))
    cutoff = split_cutoff(cleaned)
    stream_df = cleaned[cleaned["InvoiceDate"] > cutoff]          
    if args.limit:
        stream_df = stream_df.head(args.limit)
    records = stream_df.to_dict("records")

    est_hours = len(records) * args.delay / 3600
    print(f"✅ {len(records):,} incoming orders ready (after split point {cutoff})")
    if est_hours > 0.5:
        print(f"⚠️  At --delay {args.delay} this takes ~{est_hours:.1f} hours. Use --limit for a demo or --delay 0 for a full load.")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        request_timeout_ms=60000,
        max_block_ms=60000,
        retries=5,
        acks=1,
        max_in_flight_requests_per_connection=1,
    )

    print(f"🚀 Streaming to '{TOPIC_NAME}'...")
    for i, row in enumerate(records, start=1):
        description = str(row["Description"]).strip()
        invoice = str(row["Invoice"])
        payload = {
            "order_id": invoice,
            "customer_id": str(int(row["Customer ID"])),
            "stock_code": row["StockCode"],
            "product_name": description,
            "category": get_category(description),
            "price": float(row["Price"]),
            "quantity": int(row["Quantity"]),
            "total_amount": round(float(row["Price"]) * int(row["Quantity"]), 2),
            "country": str(row["Country"]),
            "payment_method": get_payment(invoice),
            "invoice_date": row["InvoiceDate"].isoformat(),                                
            "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),       
        }
        producer.send(TOPIC_NAME, key=invoice, value=payload).add_errback(on_error)

        if args.delay > 0 or i % 5000 == 0:
            print(f"[{i}/{len(records)}] {invoice} | {description} | {payload['total_amount']}")
        if args.delay > 0:
            time.sleep(args.delay)

    producer.flush()
    producer.close()
    print("🏁 Streaming finished")


if __name__ == "__main__":
    main()
    