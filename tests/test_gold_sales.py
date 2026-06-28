from __future__ import annotations

import pytest

from core import GOLD_SALES_COLUMNS, create_gold_sales


def test_gold_sales_fields_collapse_and_partitions(spark):
    df = spark.createDataFrame(
        [
            {
                "order_id": "O-1",
                "order_date": "2018-12-30",
                "ship_date": "2019-01-05",
                "ship_mode": "Standard Class",
                "city": "Amsterdam",
            },
            {
                "order_id": "O-1",
                "order_date": "2018-12-30",
                "ship_date": "2019-01-05",
                "ship_mode": "Standard Class",
                "city": "Amsterdam",
            },
        ]
    ).selectExpr(
        "order_id",
        "cast(order_date as date) as order_date",
        "cast(ship_date as date) as ship_date",
        "ship_mode",
        "city",
    )

    gold = create_gold_sales(df)
    row = gold.collect()[0]

    assert gold.columns == GOLD_SALES_COLUMNS
    assert gold.count() == 1
    assert row["shipment_date"].isoformat() == "2019-01-05"
    assert row["shipment_mode"] == "Standard Class"
    assert row["order_year"] == 2018
    assert row["order_month"] == 12
    assert row["order_day"] == 30


def test_gold_sales_conflicting_order_attributes_are_detected(spark):
    df = spark.createDataFrame(
        [
            ("O-1", "2018-12-30", "2019-01-05", "Standard Class", "Amsterdam"),
            ("O-1", "2018-12-30", "2019-01-05", "Second Class", "Amsterdam"),
        ],
        ["order_id", "order_date", "ship_date", "ship_mode", "city"],
    ).selectExpr(
        "order_id",
        "cast(order_date as date) as order_date",
        "cast(ship_date as date) as ship_date",
        "ship_mode",
        "city",
    )

    with pytest.raises(ValueError, match="Conflicting order-level attributes"):
        create_gold_sales(df)
