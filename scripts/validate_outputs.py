"""Validate locally generated outputs against the provided data/train.csv facts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


EXPECTED = {
    "bronze_rows": 9800,
    "silver_valid_rows": 9800,
    "silver_quarantine_rows": 0,
    "gold_sales_rows": 4922,
    "gold_customer_rows": 793,
    "gold_sales_csv_rows": 4922,
    "gold_customer_csv_rows": 793,
    "latest_order_date": "2018-12-30",
    "last_month_distinct_orders": 234,
    "last_six_month_distinct_orders": 1073,
    "all_time_distinct_orders": 4922,
}


def parse_args() -> argparse.Namespace:
    """Read the processed-data location to validate."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--output_root", default="data/processed")
    return parser.parse_args()


def count_distinct_orders_between(silver, latest_order_date, months_back: int) -> int:
    """Count distinct orders in a rolling month window ending at latest_order_date."""

    window_start = F.add_months(F.lit(latest_order_date).cast("date"), -months_back)
    window_end = F.lit(latest_order_date).cast("date")
    return (
        silver.filter((F.col("order_date") >= window_start) & (F.col("order_date") <= window_end))
        .select("order_id")
        .distinct()
        .count()
    )


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_root)
    spark = (
        SparkSession.builder.appName("hema-retail-sales-validation")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    try:
        # Read each generated output exactly the way a reviewer or downstream
        # consumer would read it.
        bronze = spark.read.parquet(str(output_root / "bronze"))
        silver = spark.read.parquet(str(output_root / "silver"))
        quarantine = spark.read.parquet(str(output_root / "quarantine" / "silver"))
        gold_sales = spark.read.parquet(str(output_root / "gold" / "sales"))
        gold_customer = spark.read.parquet(str(output_root / "gold" / "customer"))
        gold_sales_csv = spark.read.option("header", True).csv(str(output_root / "gold_csv" / "sales.csv"))
        gold_customer_csv = spark.read.option("header", True).csv(str(output_root / "gold_csv" / "customer.csv"))

        latest = silver.agg(F.max("order_date").alias("latest")).collect()[0]["latest"]

        # These checks pin the expected facts for the provided assessment file.
        # They are intentionally separate from unit tests so reviewers can
        # validate the generated local data with one command.
        metrics = {
            "bronze_rows": bronze.count(),
            "silver_valid_rows": silver.count(),
            "silver_quarantine_rows": quarantine.count(),
            "gold_sales_rows": gold_sales.count(),
            "gold_customer_rows": gold_customer.count(),
            "gold_sales_csv_rows": gold_sales_csv.count(),
            "gold_customer_csv_rows": gold_customer_csv.count(),
            "latest_order_date": str(latest),
            "last_month_distinct_orders": count_distinct_orders_between(silver, latest, months_back=1),
            "last_six_month_distinct_orders": count_distinct_orders_between(silver, latest, months_back=6),
            "all_time_distinct_orders": silver.select("order_id").distinct().count(),
        }
        failures = {k: {"actual": metrics[k], "expected": v} for k, v in EXPECTED.items() if metrics[k] != v}
        print(json.dumps(metrics, indent=2, sort_keys=True))
        if failures:
            raise AssertionError(json.dumps(failures, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
