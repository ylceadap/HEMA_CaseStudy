from __future__ import annotations

from core import split_bronze_by_order_date, transform_bronze


def test_bronze_normalizes_dates_and_preserves_additive_columns(spark):
    raw = spark.createDataFrame(
        [
            {
                "Row ID": "1",
                "Order Date": "08/11/2017",
                "Ship Date": "11/11/2017",
                "New Attribute": "kept",
            }
        ]
    )

    bronze = transform_bronze(raw, "run-1")
    row = bronze.collect()[0]

    assert "new_attribute" in bronze.columns
    assert row["new_attribute"] == "kept"
    assert str(row["order_date"]) == "2017-11-08"
    assert row["order_year"] == 2017
    assert row["_pipeline_run_id"] == "run-1"


def test_bronze_splits_malformed_order_dates(spark):
    raw = spark.createDataFrame(
        [
            {"Order Date": "not-a-date", "Ship Date": "11/11/2017"},
            {"Order Date": "08/11/2017", "Ship Date": "11/11/2017"},
        ]
    )

    valid, malformed = split_bronze_by_order_date(transform_bronze(raw, "run-1"))

    assert valid.count() == 1
    assert malformed.count() == 1
    assert malformed.collect()[0]["_rejection_reason"] == "order_date cannot be parsed"
