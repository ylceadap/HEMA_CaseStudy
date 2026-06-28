"""AWS Glue-compatible Silver cleansing job."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core import (
    configure_logger,
    read_parquet,
    register_parquet_table_in_catalog,
    silver_partition_columns,
    split_silver_valid_quarantine,
    write_parquet,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse paths and run metadata supplied by the Glue job trigger."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--quarantine_path", required=True)
    parser.add_argument("--catalog_database", required=False)
    parser.add_argument("--catalog_table", default="retail_sales_silver")
    parser.add_argument("--pipeline_run_id", required=True)
    parser.add_argument("--job_name", default="silver_cleansing")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    logger = configure_logger(args.job_name, args.pipeline_run_id).bind(
        input_path=args.input_path,
        output_path=args.output_path,
        quarantine_path=args.quarantine_path,
    )
    spark = SparkSession.builder.appName(args.job_name).getOrCreate()
    try:
        logger.info("job_start", status="started")

        # Silver applies the business data-quality contract. Valid rows move
        # forward; rejected rows are still written for audit and debugging.
        bronze = read_parquet(spark, args.input_path)
        input_rows = bronze.count()
        logger.info("input_read", input_row_count=input_rows, detected_schema=bronze.schema.simpleString())

        valid, rejected = split_silver_valid_quarantine(bronze)
        valid_rows = valid.count()
        rejected_rows = rejected.count()
        duplicate_rows = rejected.filter("_rejection_reason LIKE '%duplicate row_id%'").count()
        logger.info(
            "data_transformed",
            output_row_count=valid_rows,
            rejected_row_count=rejected_rows,
            duplicate_row_count=duplicate_rows,
        )

        write_parquet(valid, args.output_path, silver_partition_columns())
        logger.info("output_written", output_path=args.output_path, output_row_count=valid_rows)
        if args.catalog_database:
            register_parquet_table_in_catalog(
                spark,
                valid,
                args.catalog_database,
                args.catalog_table,
                args.output_path,
                silver_partition_columns(),
            )
            logger.info("catalog_registered", database=args.catalog_database, table=args.catalog_table)
        write_parquet(rejected, args.quarantine_path)
        logger.info(
            "output_written",
            output_path=args.quarantine_path,
            output_row_count=rejected_rows,
            dataset="silver_quarantine",
        )
        logger.info(
            "job_end",
            status="succeeded",
            input_row_count=input_rows,
            output_row_count=valid_rows,
            rejected_row_count=rejected_rows,
            duplicate_row_count=duplicate_rows,
            missing_required_columns=[],
        )
    except Exception:
        logger.exception("job_failed", status="failed")
        raise


if __name__ == "__main__":
    main()
