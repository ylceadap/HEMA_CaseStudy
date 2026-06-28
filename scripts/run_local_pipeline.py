"""Run the full local pipeline against a CSV file."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core import (
    bronze_partition_columns,
    configure_logger,
    create_gold_customer,
    create_gold_sales,
    gold_customer_partition_columns,
    gold_sales_partition_columns,
    read_csv,
    read_parquet,
    silver_partition_columns,
    split_bronze_by_order_date,
    split_silver_valid_quarantine,
    transform_bronze,
    write_csv,
    write_parquet,
)


def parse_args() -> argparse.Namespace:
    """Read local runtime options.

    The defaults make the script runnable out of the box, but `--input_path`
    can point to another CSV file or folder with the same schema.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", default="data/train.csv")
    parser.add_argument("--output_root", default="data/processed")
    parser.add_argument("--pipeline_run_id", default="local-run")
    parser.add_argument("--clean", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_root)

    if args.clean and output_root.exists():
        shutil.rmtree(output_root)

    bronze_path = output_root / "bronze"
    silver_path = output_root / "silver"
    bronze_quarantine_path = output_root / "quarantine" / "bronze_malformed_order_dates"
    silver_quarantine_path = output_root / "quarantine" / "silver"
    gold_sales_path = output_root / "gold" / "sales"
    gold_customer_path = output_root / "gold" / "customer"
    gold_sales_csv_path = output_root / "gold_csv" / "sales"
    gold_customer_csv_path = output_root / "gold_csv" / "customer"

    logger = configure_logger("local_pipeline", args.pipeline_run_id).bind(
        input_path=args.input_path,
        output_path=str(output_root),
    )
    spark = (
        SparkSession.builder.appName("hema-retail-sales-local")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    try:
        logger.info("pipeline_start", status="started")

        # Bronze: keep the source data shape, but normalize names and add
        # lineage/partition columns.
        raw = read_csv(spark, args.input_path)
        bronze = transform_bronze(raw, args.pipeline_run_id)
        bronze_valid, bronze_malformed = split_bronze_by_order_date(bronze)
        write_parquet(bronze_valid, str(bronze_path), bronze_partition_columns())
        write_parquet(bronze_malformed, str(bronze_quarantine_path))

        # Silver: cast business fields, apply data quality checks, and keep
        # rejected rows in quarantine with a readable reason.
        silver_valid, silver_rejected = split_silver_valid_quarantine(read_parquet(spark, str(bronze_path)))
        write_parquet(silver_valid, str(silver_path), silver_partition_columns())
        write_parquet(silver_rejected, str(silver_quarantine_path))

        # Gold: publish analytics-friendly tables. Parquet is the canonical
        # output; CSV is only a reviewer-friendly copy for quick inspection.
        silver = read_parquet(spark, str(silver_path))
        gold_sales = create_gold_sales(silver)
        write_parquet(gold_sales, str(gold_sales_path), gold_sales_partition_columns())
        write_csv(gold_sales, str(gold_sales_csv_path))

        gold_customer = create_gold_customer(silver)
        write_parquet(gold_customer, str(gold_customer_path), gold_customer_partition_columns())
        write_csv(gold_customer, str(gold_customer_csv_path))
        logger.info("pipeline_end", status="succeeded")
    except Exception:
        logger.exception("pipeline_failed", status="failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
