"""AWS Glue-compatible Gold Sales job."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core import (
    configure_logger,
    create_gold_sales,
    gold_sales_partition_columns,
    read_parquet,
    register_parquet_table_in_catalog,
    write_csv,
    write_parquet,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse paths for the Gold Sales job.

    `csv_output_path` is optional because Parquet is the canonical lake format;
    CSV is useful when a reviewer wants to open the result quickly.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--csv_output_path", required=False)
    parser.add_argument("--catalog_database", required=False)
    parser.add_argument("--catalog_table", default="retail_sales_gold_sales")
    parser.add_argument("--pipeline_run_id", required=True)
    parser.add_argument("--job_name", default="gold_sales")
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

        # Convert line-item Silver rows into one order-level Gold Sales table.
        silver = read_parquet(spark, args.input_path)
        input_rows = silver.count()
        logger.info("input_read", input_row_count=input_rows, detected_schema=silver.schema.simpleString())
        gold = create_gold_sales(silver)
        output_rows = gold.count()
        logger.info("data_transformed", output_row_count=output_rows)

        write_parquet(gold, args.output_path, gold_sales_partition_columns())
        logger.info("output_written", output_path=args.output_path, output_row_count=output_rows)
        if args.catalog_database:
            register_parquet_table_in_catalog(
                spark,
                gold,
                args.catalog_database,
                args.catalog_table,
                args.output_path,
                gold_sales_partition_columns(),
            )
            logger.info("catalog_registered", database=args.catalog_database, table=args.catalog_table)
        if args.csv_output_path:
            write_csv(gold, args.csv_output_path)
            logger.info(
                "output_written",
                output_path=args.csv_output_path,
                output_row_count=output_rows,
                format="csv",
            )
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
