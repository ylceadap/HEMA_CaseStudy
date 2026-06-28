"""AWS Glue-compatible Gold Customer job."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core import (
    configure_logger,
    create_gold_customer,
    gold_customer_partition_columns,
    read_parquet,
    register_parquet_table_in_catalog,
    write_csv,
    write_parquet,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse paths for the Gold Customer snapshot job.

    `csv_output_path` is optional because Parquet remains the main output
    format. The CSV copy is only for easier human inspection.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--csv_output_path", required=False)
    parser.add_argument("--catalog_database", required=False)
    parser.add_argument("--catalog_table", default="retail_sales_gold_customer")
    parser.add_argument("--pipeline_run_id", required=True)
    parser.add_argument("--job_name", default="gold_customer")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    logger = configure_logger(args.job_name, args.pipeline_run_id).bind(
        input_path=args.input_path,
        output_path=args.output_path,
        csv_output_path=args.csv_output_path,
    )
    spark = SparkSession.builder.appName(args.job_name).getOrCreate()
    try:
        logger.info("job_start", status="started")

        # Build a customer-level snapshot with all-time and rolling-window
        # order counts based on the latest order date in Silver.
        silver = read_parquet(spark, args.input_path)
        input_rows = silver.count()
        gold = create_gold_customer(silver)
        output_rows = gold.count()

        write_parquet(gold, args.output_path, gold_customer_partition_columns())
        if args.catalog_database:
            register_parquet_table_in_catalog(
                spark,
                gold,
                args.catalog_database,
                args.catalog_table,
                args.output_path,
                gold_customer_partition_columns(),
            )
        if args.csv_output_path:
            write_csv(gold, args.csv_output_path)
        logger.info(
            "job_end",
            status="succeeded",
            input_row_count=input_rows,
            output_row_count=output_rows,
        )
    except Exception:
        logger.exception("job_failed", status="failed")
        raise


if __name__ == "__main__":
    main()
