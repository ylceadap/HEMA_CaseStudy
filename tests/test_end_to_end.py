from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from core import (
    create_gold_customer,
    create_gold_sales,
    read_csv,
    split_bronze_by_order_date,
    split_silver_valid_quarantine,
    transform_bronze,
)


def count_distinct_orders_between(silver, latest_order_date, months_back: int) -> int:
    """Mirror the Gold Customer rolling-window logic for the sample dataset."""

    window_start = F.add_months(F.lit(latest_order_date).cast("date"), -months_back)
    window_end = F.lit(latest_order_date).cast("date")
    return (
        silver.filter((F.col("order_date") >= window_start) & (F.col("order_date") <= window_end))
        .select("order_id")
        .distinct()
        .count()
    )


def test_train_csv_end_to_end_validation(spark):
    input_path = Path("data/train.csv")
    if not input_path.exists():
        pytest.skip("place the Kaggle dataset at data/train.csv to run this validation")

    raw = read_csv(spark, str(input_path))
    bronze_valid, bronze_malformed = split_bronze_by_order_date(transform_bronze(raw, "test-run"))
    silver_valid, quarantine = split_silver_valid_quarantine(bronze_valid)
    gold_sales = create_gold_sales(silver_valid)
    gold_customer = create_gold_customer(silver_valid)

    latest = silver_valid.agg(F.max("order_date").alias("latest")).collect()[0]["latest"]

    assert raw.count() == 9800
    assert bronze_valid.count() == 9800
    assert bronze_malformed.count() == 0
    assert silver_valid.count() == 9800
    assert quarantine.count() == 0
    assert gold_sales.count() == 4922
    assert gold_customer.count() == 793
    assert str(latest) == "2018-12-30"
    assert count_distinct_orders_between(silver_valid, latest, months_back=1) == 234
    assert count_distinct_orders_between(silver_valid, latest, months_back=6) == 1073
    assert silver_valid.select("order_id").distinct().count() == 4922
