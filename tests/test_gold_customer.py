from __future__ import annotations

from core import create_gold_customer


def test_gold_customer_name_counts_boundaries_and_snapshot(spark):
    df = spark.createDataFrame(
        [
            ("C-1", "Darrin Van Huff", "Corporate", "United States", "O-1", "2018-12-30"),
            ("C-1", "Darrin Van Huff", "Corporate", "United States", "O-1", "2018-12-30"),
            ("C-1", "Darrin Van Huff", "Corporate", "United States", "O-2", "2018-11-30"),
            ("C-1", "Darrin Van Huff", "Corporate", "United States", "O-3", "2018-06-30"),
            ("C-1", "Darrin Van Huff", "Corporate", "United States", "O-4", "2018-06-29"),
            ("C-2", "Corey-Lock", "Consumer", "United States", "O-5", "2018-01-01"),
        ],
        ["customer_id", "customer_name", "segment", "country", "order_id", "order_date"],
    ).selectExpr(
        "customer_id",
        "customer_name",
        "segment",
        "country",
        "order_id",
        "cast(order_date as date) as order_date",
    )

    gold = create_gold_customer(df)
    rows = {row["customer_id"]: row.asDict() for row in gold.collect()}

    assert rows["C-1"]["customer_first_name"] == "Darrin"
    assert rows["C-1"]["customer_last_name"] == "Van Huff"
    assert rows["C-1"]["orders_last_month"] == 2
    assert rows["C-1"]["orders_last_6_months"] == 3
    assert rows["C-1"]["orders_all_time"] == 4
    assert rows["C-1"]["snapshot_date"].isoformat() == "2018-12-30"
    assert rows["C-1"]["snapshot_year"] == 2018
    assert rows["C-1"]["snapshot_month"] == 12
    assert rows["C-1"]["snapshot_day"] == 30
    assert rows["C-2"]["customer_first_name"] == "Corey-Lock"
    assert rows["C-2"]["customer_last_name"] is None
    assert rows["C-2"]["orders_last_month"] == 0
    assert rows["C-2"]["orders_last_6_months"] == 0
    assert rows["C-2"]["orders_all_time"] == 1
