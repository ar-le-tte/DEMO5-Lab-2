import argparse
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_timestamp, current_timestamp, lit, when, regexp_replace)
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType)

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
    ap.add_argument("--env_path", default="config/.env")
    ap.add_argument("--trigger_seconds", type=int, default=10)
    ap.add_argument("--run_seconds", type=int, default=0, help="0 = run forever")
    args = ap.parse_args()

    env = load_env(args.env_path)

    pg_url = env["PG_JDBC_URL"]
    pg_user = env["PG_USER"]
    pg_password = env["PG_PASSWORD"]
    pg_schema = env.get("PG_SCHEMA", "rt")
    pg_table = env.get("PG_TABLE", "ecommerce_events")
    full_table = f"{pg_schema}.{pg_table}"

    spark = SparkSession.builder.appName("lab2-stream-to-postgres").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True), StructField("user_id", IntegerType(), True),
        StructField("session_id", StringType(), True), StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True), StructField("product_name", StringType(), True),
        StructField("category", StringType(), True), StructField("price", StringType(), True),
        StructField("quantity", IntegerType(), True), StructField("total_amount", StringType(), True),])

    raw = (spark.readStream.format("csv").option("header", "true")
        .schema(schema).load(args.input_dir))

    df = ( raw.withColumn("event_time", to_timestamp(col("event_time"))).withColumn("event_type", regexp_replace(col("event_type"), r"\s+", ""))
        .withColumn("price", regexp_replace(col("price"), r"[^0-9.]", "").cast("double")).withColumn("total_amount", regexp_replace(col("total_amount"), r"[^0-9.]", "").cast("double"))
        .withColumn("quantity", when(col("quantity").isNull(), lit(1)).otherwise(col("quantity")))
        .withColumn("total_amount", when(col("event_type") == lit("view"), lit(0.0)).otherwise(col("total_amount"))
        ).withColumn("ingest_time", current_timestamp()).filter(col("event_id").isNotNull())
        .filter(col("event_time").isNotNull()).filter(col("event_type").isin("view", "purchase"))
    )

    metrics = {"rows": 0, "start": time.time()}

    def write_batch(batch_df, batch_id: int):
        t0 = time.time()
        count = batch_df.count()
        if count == 0:
            return
        (
            batch_df.write
            .format("jdbc")
            .option("url", pg_url)
            .option("dbtable", full_table)
            .option("user", pg_user)
            .option("password", pg_password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        metrics["rows"] += count
        elapsed = time.time() - metrics["start"]
        throughput = metrics["rows"] / elapsed if elapsed > 0 else 0.0
        print(f"[batch {batch_id}] rows={count} total={metrics['rows']} throughput={throughput:.1f} rows/s")
        print(f"... batch_time={(time.time()-t0):.2f}s")


    query = (df.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )
    if args.run_seconds > 0:
        query.awaitTermination(args.run_seconds)
        query.stop()
    else:
        query.awaitTermination()


if __name__ == "__main__":
    main()
