"""Core PySpark logic for the HEMA retail sales pipeline.

This file is intentionally independent from AWS Glue. Glue entry-point scripts,
local scripts, and tests import these functions so the transformation logic is
reusable and locally testable.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Source dates in the provided CSV use day/month/year, for example 30/12/2018.
SOURCE_DATE_FORMAT = "dd/MM/yyyy"

# These are the minimum business fields Silver needs in order to validate rows
# and build the two Gold tables. Bronze can keep extra source columns.
REQUIRED_SILVER_COLUMNS = [
    "row_id",
    "order_id",
    "order_date",
    "ship_date",
    "ship_mode",
    "customer_id",
    "customer_name",
    "segment",
    "country",
    "city",
    "sales",
]

# The assessment asks for order-date partitioning by year, month, and day.
PARTITION_COLUMNS = ["order_year", "order_month", "order_day"]
SNAPSHOT_PARTITION_COLUMNS = ["snapshot_year", "snapshot_month", "snapshot_day"]

# Gold Sales collapses product-line rows into one row per order, so these fields
# must not disagree within the same order_id.
ORDER_LEVEL_COLUMNS = ["order_date", "ship_date", "ship_mode", "city"]
GOLD_SALES_COLUMNS = [
    "order_id",
    "order_date",
    "shipment_date",
    "shipment_mode",
    "city",
    "order_year",
    "order_month",
    "order_day",
]
GOLD_CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_first_name",
    "customer_last_name",
    "customer_segment",
    "country",
    "orders_last_month",
    "orders_last_6_months",
    "orders_all_time",
    "snapshot_date",
    "snapshot_year",
    "snapshot_month",
    "snapshot_day",
]


class JsonFormatter(logging.Formatter):
    """Format log records as compact JSON suitable for CloudWatch Logs."""

    def format(self, record: logging.LogRecord) -> str:
        """Turn one Python log record into a JSON string."""

        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in getattr(record, "context", {}).items():
            payload[key] = value
        if record.exc_info:
            payload["stack_trace"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, sort_keys=True)


class ContextLogger:
    """Small adapter that adds job context to every structured log line."""

    def __init__(self, logger: logging.Logger, **context: Any) -> None:
        """Store a normal Python logger together with fixed job metadata."""

        self._logger = logger
        self._context = context

    def bind(self, **context: Any) -> "ContextLogger":
        """Return a new logger with extra fields added to every future log line."""

        merged = dict(self._context)
        merged.update(context)
        return ContextLogger(self._logger, **merged)

    def info(self, message: str, **context: Any) -> None:
        """Write an informational log line with optional extra fields."""

        self._log(logging.INFO, message, **context)

    def error(self, message: str, **context: Any) -> None:
        """Write an error log line with optional extra fields."""

        self._log(logging.ERROR, message, **context)

    def exception(self, message: str, **context: Any) -> None:
        """Write an error log line and include the current exception stack trace."""

        self._log(logging.ERROR, message, exc_info=True, **context)

    def _log(self, level: int, message: str, **context: Any) -> None:
        """Merge fixed job metadata with event-specific fields and log once."""

        merged = dict(self._context)
        merged.update(context)
        self._logger.log(level, message, extra={"context": merged})


def configure_logger(
    job_name: str,
    pipeline_run_id: str,
    level: int = logging.INFO,
) -> ContextLogger:
    """Create the shared JSON logger used by local scripts and Glue jobs.

    Every log line gets the job name and pipeline run id, so a production
    operator can trace all messages from one daily run.
    """

    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        root.addHandler(handler)
    else:
        for handler in root.handlers:
            handler.setFormatter(JsonFormatter())
    root.setLevel(level)
    return ContextLogger(
        logging.getLogger("hema_retail_sales"),
        job_name=job_name,
        pipeline_run_id=pipeline_run_id,
    )


def read_csv(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the source CSV with the options needed for this dataset.

    The file has a header row and some quoted product names with commas, so the
    reader is configured to handle quoted multi-line CSV safely.
    """

    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("quote", '"')
        .csv(input_path)
    )


def read_parquet(spark: SparkSession, input_path: str) -> DataFrame:
    """Read a Parquet dataset from a local path or an S3 path."""

    return spark.read.parquet(input_path)


def write_parquet(
    df: DataFrame,
    output_path: str,
    partition_by: list[str] | None = None,
    mode: str = "overwrite",
) -> None:
    """Write a DataFrame as Parquet, optionally partitioned by given columns.

    This keeps local development and AWS Glue writes using the same code path.
    """

    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(output_path)


def write_csv(
    df: DataFrame,
    output_path: str,
    mode: str = "overwrite",
    single_file: bool = True,
) -> None:
    """Write a headered CSV copy for quick manual inspection.

    Spark writes CSV as a folder rather than one named file. With
    `single_file=True`, the folder contains one `part-...csv` file, which is
    easier for reviewers to open locally.
    """

    output_df = df.coalesce(1) if single_file else df
    output_df.write.mode(mode).option("header", True).csv(output_path)


def register_parquet_table_in_catalog(
    spark: SparkSession,
    df: DataFrame,
    database: str,
    table: str,
    location: str,
    partition_by: list[str] | None = None,
) -> None:
    """Register an external Parquet dataset in the AWS Glue Data Catalog.

    AWS Glue Spark sessions use the Glue Data Catalog as the Hive metastore.
    This function is intentionally optional for local runs; Glue jobs call it
    only when catalog arguments are provided.
    """

    partition_columns = partition_by or []
    partition_set = set(partition_columns)
    table_columns = [field for field in df.schema.fields if field.name not in partition_set]
    partition_fields = [field for field in df.schema.fields if field.name in partition_set]
    escaped_location = location.replace("'", "\\'")

    columns_sql = ",\n  ".join(
        f"`{field.name}` {field.dataType.simpleString()}" for field in table_columns
    )
    partition_sql = ""
    if partition_fields:
        partition_defs = ", ".join(
            f"`{field.name}` {field.dataType.simpleString()}" for field in partition_fields
        )
        partition_sql = f"\nPARTITIONED BY ({partition_defs})"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (
  {columns_sql}
)
USING PARQUET{partition_sql}
LOCATION '{escaped_location}'
"""
    )
    if partition_fields:
        spark.sql(f"MSCK REPAIR TABLE `{database}`.`{table}`")


def normalize_column_name(name: str) -> str:
    """Convert a source column name into lower snake_case.

    Example: `Order Date` becomes `order_date`, and `Sub-Category` becomes
    `sub_category`.
    """

    normalized = re.sub(r"[^0-9A-Za-z]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def normalize_columns(df: DataFrame) -> DataFrame:
    """Normalize every column name while preserving all source columns.

    Bronze should not silently drop new incoming attributes, so this function
    renames all columns instead of selecting only a fixed known list.
    """

    result = df
    seen: set[str] = set()
    for original in df.columns:
        normalized = normalize_column_name(original)
        candidate = normalized
        index = 2
        while candidate in seen:
            candidate = f"{normalized}_{index}"
            index += 1
        seen.add(candidate)
        if original != candidate:
            result = result.withColumnRenamed(original, candidate)
    return result


def require_columns(df: DataFrame, required_columns: list[str]) -> None:
    """Fail fast if a DataFrame is missing columns required by a layer."""

    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def _add_order_date_partitions(df: DataFrame, date_column: str = "order_date") -> DataFrame:
    """Add year/month/day partition columns from one date column.

    Keeping this in one helper makes it clear that Bronze, Silver, and Gold
    Sales use the same partitioning rule required by the assignment.
    """

    return (
        df.withColumn("order_year", F.year(date_column))
        .withColumn("order_month", F.month(date_column))
        .withColumn("order_day", F.dayofmonth(date_column))
    )


def transform_bronze(df: DataFrame, pipeline_run_id: str) -> DataFrame:
    """Create the Bronze dataset from raw CSV rows.

    Bronze keeps source attributes, standardizes column names, parses the two
    date fields, adds lineage metadata, and creates order-date partition fields.
    """

    normalized = normalize_columns(df)
    require_columns(normalized, ["order_date", "ship_date"])

    # Bronze is still close to raw, but parsed dates and lineage columns make it
    # much easier to debug where each row came from.
    with_dates_and_lineage = (
        normalized.withColumn(
            "order_date",
            F.coalesce(F.to_date(F.col("order_date"), SOURCE_DATE_FORMAT), F.col("order_date").cast("date")),
        )
        .withColumn(
            "ship_date",
            F.coalesce(F.to_date(F.col("ship_date"), SOURCE_DATE_FORMAT), F.col("ship_date").cast("date")),
        )
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
    )
    return _add_order_date_partitions(with_dates_and_lineage)


def split_bronze_by_order_date(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate rows with a usable order date from rows with a bad order date.

    Rows with a malformed `order_date` cannot be partitioned by order date, so
    they are sent to quarantine instead of being silently dropped.
    """

    valid = df.filter(F.col("order_date").isNotNull())
    malformed = df.filter(F.col("order_date").isNull()).withColumn(
        "_rejection_reason", F.lit("order_date cannot be parsed")
    )
    return valid, malformed


def bronze_partition_columns() -> list[str]:
    """Return the partition columns used by the Bronze dataset."""

    return list(PARTITION_COLUMNS)


def _trim_string_columns(df: DataFrame) -> DataFrame:
    """Remove leading and trailing spaces from every string column."""

    result = df
    for field in result.schema.fields:
        if field.dataType.simpleString() == "string":
            result = result.withColumn(field.name, F.trim(F.col(field.name)))
    return result


def _parse_date(column_name: str):
    """Build a Spark expression that parses a date column consistently.

    It first tries the source format `dd/MM/yyyy`, then falls back to Spark's
    normal date cast for data that may already be typed as a date.
    """

    return F.coalesce(
        F.to_date(F.col(column_name).cast("string"), SOURCE_DATE_FORMAT),
        F.col(column_name).cast("date"),
    )


def prepare_silver(df: DataFrame) -> DataFrame:
    """Apply Silver typing and basic cleanup before record-level validation.

    This is where known business fields become useful types, such as `row_id`
    as long, `sales` as double, and date columns as Spark dates.
    """

    require_columns(df, REQUIRED_SILVER_COLUMNS)
    typed = (
        _trim_string_columns(df)
        .withColumn("row_id", F.col("row_id").cast("long"))
        .withColumn("order_date", _parse_date("order_date"))
        .withColumn("ship_date", _parse_date("ship_date"))
        .withColumn("sales", F.col("sales").cast("double"))
    )
    return _add_order_date_partitions(typed)


def split_silver_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split Silver rows into valid records and rejected quarantine records.

    The function checks required business rules, marks duplicate `row_id`
    occurrences deterministically, and adds a readable `_rejection_reason` for
    every rejected row.
    """

    prepared = prepare_silver(df)

    # If duplicate row_id values appear, rank them deterministically using a
    # hash of the row payload. The first row is kept; the rest go to quarantine.
    payload_columns = [column for column in prepared.columns if column != "_rejection_reason"]
    dedupe_values = [F.coalesce(F.col(column).cast("string"), F.lit("<null>")) for column in payload_columns]
    with_dedupe_hash = prepared.withColumn(
        "_dedupe_hash",
        F.sha2(F.concat_ws("||", *dedupe_values), 256),
    )
    row_id_window = Window.partitionBy("row_id").orderBy(F.col("_dedupe_hash").asc())
    ranked = with_dedupe_hash.withColumn("_row_id_rank", F.row_number().over(row_id_window))

    row_id_missing = F.col("row_id").isNull()
    order_id_missing = F.col("order_id").isNull() | (F.col("order_id") == "")
    order_date_invalid = F.col("order_date").isNull()
    ship_date_invalid = F.col("ship_date").isNull()
    customer_id_missing = F.col("customer_id").isNull() | (F.col("customer_id") == "")
    customer_name_missing = F.col("customer_name").isNull() | (F.col("customer_name") == "")
    ship_before_order = F.col("ship_date") < F.col("order_date")
    sales_not_positive = F.col("sales").isNull() | (F.col("sales") <= 0)
    is_duplicate = F.col("row_id").isNotNull() & (F.col("_row_id_rank") > 1)

    # concat_ws skips null expressions, so valid rows receive an empty string
    # and rejected rows receive one or more readable rejection reasons.
    reason = F.concat_ws(
        "; ",
        F.when(row_id_missing, F.lit("row_id is null")),
        F.when(order_id_missing, F.lit("order_id is null")),
        F.when(order_date_invalid, F.lit("order_date cannot be parsed")),
        F.when(ship_date_invalid, F.lit("ship_date cannot be parsed")),
        F.when(customer_id_missing, F.lit("customer_id is null")),
        F.when(customer_name_missing, F.lit("customer_name is null or blank")),
        F.when(ship_before_order, F.lit("ship_date is earlier than order_date")),
        F.when(sales_not_positive, F.lit("sales is null or not greater than zero")),
        F.when(is_duplicate, F.lit("duplicate row_id")),
    )

    evaluated = ranked.withColumn("_rejection_reason", reason)
    technical_columns = ["_dedupe_hash", "_row_id_rank"]
    valid = evaluated.filter(F.col("_rejection_reason") == "").drop("_rejection_reason", *technical_columns)
    rejected = evaluated.filter(F.col("_rejection_reason") != "").drop(*technical_columns)
    return valid, rejected


def silver_partition_columns() -> list[str]:
    """Return the partition columns used by the Silver dataset."""

    return list(PARTITION_COLUMNS)


def validate_order_level_consistency(df: DataFrame) -> None:
    """Make sure each order has one consistent set of order-level attributes.

    The source is product-line level, so one order can appear on multiple rows.
    Before collapsing to one row per order, we verify fields like ship mode and
    city do not conflict within the same order.
    """

    require_columns(df, ["order_id", *ORDER_LEVEL_COLUMNS])
    checks = df.groupBy("order_id").agg(
        *[F.countDistinct(F.col(column)).alias(f"{column}_distinct_count") for column in ORDER_LEVEL_COLUMNS]
    )
    conflict_filter = " OR ".join([f"{column}_distinct_count > 1" for column in ORDER_LEVEL_COLUMNS])
    conflicts = checks.filter(conflict_filter)
    examples = conflicts.limit(10).collect()
    if examples:
        order_ids = ", ".join(str(row["order_id"]) for row in examples)
        raise ValueError(f"Conflicting order-level attributes detected for order_id(s): {order_ids}")


def create_gold_sales(df: DataFrame) -> DataFrame:
    """Create the Gold Sales dataset with one row per distinct order."""

    validate_order_level_consistency(df)

    # The source file is line-item level. Gold Sales is order level, so each
    # order_id becomes one row after the consistency check above.
    order_level = (
        df.groupBy("order_id")
        .agg(
            F.first("order_date").alias("order_date"),
            F.first("ship_date").alias("shipment_date"),
            F.first("ship_mode").alias("shipment_mode"),
            F.first("city").alias("city"),
        )
    )
    return _add_order_date_partitions(order_level).select(*GOLD_SALES_COLUMNS)


def gold_sales_partition_columns() -> list[str]:
    """Return the partition columns used by the Gold Sales dataset."""

    return list(PARTITION_COLUMNS)


def _split_customer_name(name_col):
    """Split a customer name into first name and last name Spark expressions.

    The first whitespace-delimited token becomes first name. Everything after
    it becomes last name. This is deterministic, even though real names are more
    complex in production systems.
    """

    tokens = F.split(F.trim(name_col), r"\s+")
    first_name = F.element_at(tokens, 1)
    last_name = F.when(F.size(tokens) > 1, F.array_join(F.slice(tokens, 2, 100), " "))
    return first_name, last_name


def create_gold_customer(df: DataFrame) -> DataFrame:
    """Create the Gold Customer snapshot dataset with one row per customer.

    Order counts are distinct `order_id` counts. The rolling windows end at the
    latest order date in the dataset, which makes the calculation reproducible
    without hardcoding a snapshot date.
    """

    require_columns(df, ["customer_id", "customer_name", "segment", "country", "order_id", "order_date"])

    latest_order_date = df.agg(F.max("order_date").alias("latest_order_date")).collect()[0]["latest_order_date"]
    if latest_order_date is None:
        raise ValueError("Cannot create Gold Customer because no latest order_date is available")

    # A customer can buy multiple products in one order. For customer-level
    # order counts, each customer/order pair should be counted once.
    customer_orders = df.dropDuplicates(["customer_id", "order_id"])
    first_name, last_name = _split_customer_name(F.col("customer_name"))
    enriched = (
        customer_orders.withColumn("_latest_order_date", F.lit(latest_order_date).cast("date"))
        .withColumn("customer_first_name", first_name)
        .withColumn("customer_last_name", last_name)
    )

    in_last_month = (F.col("order_date") >= F.add_months(F.col("_latest_order_date"), -1)) & (
        F.col("order_date") <= F.col("_latest_order_date")
    )
    in_last_six_months = (F.col("order_date") >= F.add_months(F.col("_latest_order_date"), -6)) & (
        F.col("order_date") <= F.col("_latest_order_date")
    )

    return (
        enriched.groupBy("customer_id")
        .agg(
            F.min("customer_first_name").alias("customer_first_name"),
            F.min("customer_last_name").alias("customer_last_name"),
            F.min("segment").alias("customer_segment"),
            F.min("country").alias("country"),
            F.countDistinct(
                F.when(in_last_month, F.col("order_id"))
            ).alias("orders_last_month"),
            F.countDistinct(
                F.when(in_last_six_months, F.col("order_id"))
            ).alias("orders_last_6_months"),
            F.countDistinct("order_id").alias("orders_all_time"),
        )
        .withColumn("snapshot_date", F.lit(latest_order_date).cast("date"))
        .withColumn("snapshot_year", F.year("snapshot_date"))
        .withColumn("snapshot_month", F.month("snapshot_date"))
        .withColumn("snapshot_day", F.dayofmonth("snapshot_date"))
        .select(*GOLD_CUSTOMER_COLUMNS)
    )


def gold_customer_partition_columns() -> list[str]:
    """Return the partition columns used by the Gold Customer snapshot."""

    return list(SNAPSHOT_PARTITION_COLUMNS)
