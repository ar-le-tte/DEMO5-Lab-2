import argparse
import os
import time
import json
from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_timestamp, current_timestamp, lit, when, regexp_replace, 
                                   expr)
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType)

# ----------------------------
# Config loading (.env)
# ----------------------------
def load_env(env_path: str) -> dict:
    env = {}
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", default="data/incoming")
    ap.add_argument("--checkpoint_dir", default="data/checkpoint")
    ap.add_argument("--bad_records_dir", default="data/bad_records")
    ap.add_argument("--metrics_out", default="reports/stream_metrics.jsonl")
    ap.add_argument("--env_path", default="config/.env")
    ap.add_argument("--trigger_seconds", type=int, default=10)
    ap.add_argument("--run_seconds", type=int, default=0, help="0 = run forever")
    
    ap.add_argument("--watermark", default="10 minutes")
    ap.add_argument("--jdbc_batchsize", type=int, default=5000)
    ap.add_argument("--write_partitions", type=int, default=4)
    args = ap.parse_args()

    # Ensure output dirs exist
    Path(args.checkpoint_dir).mkdir(parents=True, exist_ok=True)
    Path(args.bad_records_dir).mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(args.metrics_out) or ".").mkdir(parents=True, exist_ok=True)

    env = load_env(args.env_path)

    pg_url = env["PG_JDBC_URL"]
    pg_user = env["PG_USER"]
    pg_password = env["PG_PASSWORD"]
    pg_schema = env.get("PG_SCHEMA", "rt")
    pg_table = env.get("PG_TABLE", "ecommerce_events")
    full_table = f"{pg_schema}.{pg_table}"

    spark = SparkSession.builder.appName("lab2-stream-to-postgres").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # --------------------------------
    # 1) ReadStream with fixed schema
    # -------------------------------
    schema = StructType([StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True), StructField("user_id", IntegerType(), True),
        StructField("session_id", StringType(), True), StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True), StructField("product_name", StringType(), True),
        StructField("category", StringType(), True), StructField("price", StringType(), True),
        StructField("quantity", IntegerType(), True), StructField("total_amount", StringType(), True),])

    raw = (spark.readStream.format("csv").option("header", "true")
        .schema(schema).load(args.input_dir))
    # --------------------------------
    # 2) Cleaning + type conversions
    # --------------------------------
    cleaned = ( raw.withColumn("event_time", to_timestamp(col("event_time"))).withColumn("event_type", regexp_replace(col("event_type"), r"\s+", ""))
        .withColumn("price", regexp_replace(col("price"), r"[^0-9.]", "").cast("double")).withColumn("total_amount", regexp_replace(col("total_amount"), r"[^0-9.]", "").cast("double"))
        .withColumn("quantity", when(col("quantity").isNull(), lit(1)).otherwise(col("quantity")))
        .withColumn("total_amount", when(col("event_type") == lit("view"), lit(0.0)).otherwise(col("total_amount")))
        .withColumn("ingest_time", current_timestamp()))

    # --------------------------------
    # 3) Data quality checks (DQ)
    #    - mark bad rows instead of only dropping
    # --------------------------------
    dq = (cleaned.withColumn("dq_error", when(col("event_id").isNull() | (col("event_id") == ""), lit("missing_event_id"))
            .when(col("event_time").isNull(), lit("bad_event_time"))
            .when(~col("event_type").isin("view", "purchase"), lit("bad_event_type"))
            .when(col("user_id").isNull(), lit("missing_user_id"))
            .when(col("product_id").isNull(), lit("missing_product_id"))
            .when(col("price").isNull() | (col("price") < 0), lit("bad_price"))
            .when(col("quantity").isNull() | (col("quantity") < 1), lit("bad_quantity"))
            .when(col("total_amount").isNull() | (col("total_amount") < 0), lit("bad_total_amount"))
            # Consistency: purchase should have total_amount ~= price*quantity
            .when((col("event_type") == lit("purchase")) &
                (expr("abs(total_amount - (price * quantity))") > 0.01),
                lit("purchase_amount_mismatch"))
            .otherwise(lit(None)))
    )

    good = dq.filter(col("dq_error").isNull()).drop("dq_error")
    bad = dq.filter(col("dq_error").isNotNull())

    # Writing bad records to parquet for inspection
    bad_query = (bad.writeStream.format("parquet").option("path", args.bad_records_dir)
        .option("checkpointLocation", os.path.join(args.checkpoint_dir, "_bad_records"))
        .outputMode("append").trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )
    # --------------------------------
    # 4) Deduplication with watermark
    # --------------------------------
    deduped = (good.withWatermark("event_time", args.watermark)
        .dropDuplicates(["event_id"]))

    def write_batch(batch_df, batch_id: int):
        t0 = time.time()
        count = batch_df.count()
        if count == 0:   # to avoid empty writes
            return
        batch_df = batch_df.repartition(args.write_partitions)
        (
            batch_df.write
            .format("jdbc")
            .option("url", pg_url)
            .option("dbtable", full_table)
            .option("user", pg_user)
            .option("password", pg_password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", str(args.jdbc_batchsize))
            .mode("append")
            .save()
        )
        batch_time = time.time() - t0
        # Basic latency estimate inside Spark (seconds)
        # This approximates event->ingest delay for this micro-batch.
        lat = batch_df.selectExpr("avg(unix_timestamp(ingest_time) - unix_timestamp(event_time)) as avg_lat") \
                      .collect()[0]["avg_lat"]

        metric = {
            "batch_id": batch_id,
            "rows": int(count),
            "batch_time_sec": float(batch_time),
            "rows_per_sec": float(count / batch_time) if batch_time > 0 else None,
            "avg_event_latency_sec": float(lat) if lat is not None else None,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }

        print(f"[batch {batch_id}] rows={count} time={batch_time:.2f}s rps={metric['rows_per_sec']:.1f} avg_lat={metric['avg_event_latency_sec']}")
        # Export metrics (JSON Lines)
        with open(args.metrics_out, "a", encoding="utf-8") as f:
            f.write(json.dumps(metric) + "\n")

    query = (
        deduped.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", os.path.join(args.checkpoint_dir, "_main"))
        .outputMode("append")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    if args.run_seconds > 0:
        query.awaitTermination(args.run_seconds)
        query.stop()
        bad_query.stop()
    else:
        query.awaitTermination()

if __name__ == "__main__":
    main()