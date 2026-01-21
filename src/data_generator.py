"""
data_generator.py
Generates fake e-commerce events as CSV files into a watched folder.

Usage:
  python data_generator.py --out_dir data/incoming --files_per_min 6 --rows_per_file 200
"""

import argparse
import csv
import os
import random
import time
import uuid
from datetime import datetime, timezone

PRODUCTS = [(101, "Wireless Mouse", "Electronics", 19.99), (102, "Mechanical Keyboard", "Electronics", 79.99),
    (103, "Water Bottle", "Home", 12.50), (104, "Running Shoes", "Sports", 59.90),
    (105, "Notebook", "Office", 3.99), (106, "Backpack", "Travel", 34.95),
    (107, "Coffee Beans", "Grocery", 14.25), (108, "Headphones", "Electronics", 49.99),]

EVENT_TYPES = ["view", "purchase"]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_event():
    product_id, product_name, category, price = random.choice(PRODUCTS)
    event_type = random.choices(EVENT_TYPES, weights=[0.80, 0.20])[0]  # more views than purchases
    qty = 1 if event_type == "view" else random.choice([1, 1, 1, 2, 3])
    total_amount = round(price * qty, 2)

    return {"event_id": str(uuid.uuid4()), "event_time": now_iso(),
        "user_id": random.randint(1, 5000), "session_id": str(uuid.uuid4()),"event_type": event_type,
        "product_id": product_id, "product_name": product_name, "category": category,
        "price": f"{price:.2f}", "quantity": str(qty), "total_amount": f"{total_amount:.2f}",}

def write_csv(path, rows):
    fieldnames = ["event_id","event_time","user_id","session_id","event_type",
        "product_id","product_name","category","price","quantity","total_amount"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for _ in range(rows):
            w.writerow(make_event())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", default="data/incoming")
    ap.add_argument("--files_per_min", type=int, default=6)     # 6 => one file every 10 seconds
    ap.add_argument("--rows_per_file", type=int, default=200)
    ap.add_argument("--run_minutes", type=int, default=0, help="0 = run forever")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    interval = 60.0 / max(args.files_per_min, 1)
    start = time.time()
    file_count = 0
    print(f"[generator] Writing CSVs to: {os.path.abspath(args.out_dir)}")
    print(f"[generator] Every {interval:.1f}s | rows/file={args.rows_per_file} | run_minutes={args.run_minutes}")

    while True:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tmp_path = os.path.join(args.out_dir, f"events_{ts}_{file_count}.tmp")
        final_path = os.path.join(args.out_dir, f"events_{ts}_{file_count}.csv")
        # Writing to tmp then atomically renaming to avoid Spark reading partial files
        write_csv(tmp_path, args.rows_per_file)
        os.replace(tmp_path, final_path)

        file_count += 1
        time.sleep(interval)

        if args.run_minutes > 0 and (time.time() - start) >= args.run_minutes * 60:
            print("[generator] Done.")
            break

if __name__ == "__main__":
    main()
