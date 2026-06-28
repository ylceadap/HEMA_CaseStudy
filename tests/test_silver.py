from __future__ import annotations

import pytest

from core import require_columns, split_silver_valid_quarantine
from tests.conftest import base_silver_row


def test_silver_parses_dd_mm_yyyy_dates(silver_source_df):
    valid, rejected = split_silver_valid_quarantine(silver_source_df([base_silver_row()]))

    row = valid.collect()[0]
    assert str(row["order_date"]) == "2017-11-08"
    assert str(row["ship_date"]) == "2017-11-11"
    assert rejected.count() == 0


def test_required_column_validation(silver_source_df):
    with pytest.raises(ValueError, match="Missing required columns"):
        require_columns(silver_source_df([{"row_id": "1"}]), ["row_id", "order_id"])


def test_deterministic_row_id_deduplication(silver_source_df):
    rows = [
        base_silver_row(row_id="1", order_id="O-1", sales="10.00"),
        base_silver_row(row_id="1", order_id="O-2", sales="20.00"),
    ]

    valid_first, rejected_first = split_silver_valid_quarantine(silver_source_df(rows))
    valid_second, rejected_second = split_silver_valid_quarantine(silver_source_df(rows))

    assert valid_first.count() == 1
    assert rejected_first.count() == 1
    assert rejected_first.collect()[0]["_rejection_reason"] == "duplicate row_id"
    assert valid_first.collect()[0]["order_id"] == valid_second.collect()[0]["order_id"]
    assert rejected_second.count() == 1


def test_invalid_ship_date_rejection(silver_source_df):
    valid, rejected = split_silver_valid_quarantine(
        silver_source_df([base_silver_row(order_date="11/11/2017", ship_date="08/11/2017")])
    )

    assert valid.count() == 0
    assert "ship_date is earlier" in rejected.collect()[0]["_rejection_reason"]


@pytest.mark.parametrize("customer_name", [None, "   "])
def test_null_or_blank_customer_rejection(silver_source_df, customer_name):
    valid, rejected = split_silver_valid_quarantine(
        silver_source_df([base_silver_row(customer_name=customer_name)])
    )

    assert valid.count() == 0
    assert "customer_name is null or blank" in rejected.collect()[0]["_rejection_reason"]


@pytest.mark.parametrize("sales", ["0", "-1", None])
def test_nonpositive_sales_rejection(silver_source_df, sales):
    valid, rejected = split_silver_valid_quarantine(silver_source_df([base_silver_row(sales=sales)]))

    assert valid.count() == 0
    assert "sales is null or not greater than zero" in rejected.collect()[0]["_rejection_reason"]


def test_nullable_postal_code_is_accepted(silver_source_df):
    valid, rejected = split_silver_valid_quarantine(silver_source_df([base_silver_row(postal_code=None)]))

    assert valid.count() == 1
    assert rejected.count() == 0


def test_unexpected_additive_column_is_preserved(silver_source_df):
    valid, _ = split_silver_valid_quarantine(
        silver_source_df([base_silver_row(promotion_code="SUMMER")])
    )

    assert "promotion_code" in valid.columns
    assert valid.collect()[0]["promotion_code"] == "SUMMER"
