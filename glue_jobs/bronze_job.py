"""AWS Glue-compatible Bronze ingestion job."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core import (
    bronze_partition_columns,
    configure_logger,
    read_csv,
    register_parquet_table_in_catalog,
    split_bronze_by_order_date,
    transform_bronze,
    write_parquet,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse Glue job arguments.

    The same entry point also works locally, which is why plain argparse is
    used instead of Glue-specific helpers.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--quarantine_path", required=False)
    parser.add_argument("--catalog_database", required=False)
    parser.add_argument("--catalog_table", default="retail_sales_bronze")
    parser.add_argument("--pipeline_run_id", required=True)
    parser.add_argument("--job_name", default="bronze_ingestion")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    logger = configure_logger(args.job_name, args.pipeline_run_id).bind(
        input_path=args.input_path,
        output_path=args.output_path,
    )
    spark = SparkSession.builder.appName(args.job_name).getOrCreate()
    try:
        logger.info("job_start", status="started")

        # Bronze keeps raw business columns, but normalizes names and adds
        # lineage/partition fields so later layers are easier to operate.
        raw = read_csv(spark, args.input_path)
        input_rows = raw.count()
        logger.info("input_read", input_row_count=input_rows, detected_columns=raw.columns)

        bronze = transform_bronze(raw, args.pipeline_run_id)
        valid, malformed = split_bronze_by_order_date(bronze)
        valid_rows = valid.count()
        malformed_rows = malformed.count()

        write_parquet(valid, args.output_path, bronze_partition_columns())
        if args.catalog_database:
            register_parquet_table_in_catalog(
                spark,
                valid,
                args.catalog_database,
                args.catalog_table,
                args.output_path,
                bronze_partition_columns(),
            )
        if args.quarantine_path and malformed_rows:
            write_parquet(malformed, args.quarantine_path)
        logger.info(
            "job_end",
            status="succeeded",
            input_row_count=input_rows,
            output_row_count=valid_rows,
            rejected_row_count=malformed_rows,
        )
    except Exception:
        logger.exception("job_failed", status="failed")
        raise


if __name__ == "__main__":
    main()
