from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create one lightweight local Spark session shared by all tests."""

    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.appName("hema-retail-sales-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def base_silver_row(**overrides):
    """Return a valid Silver-like source row, with optional field overrides.

    Most Silver tests only care about one broken field at a time. Starting from
    a known-good row keeps those tests small and readable.
    """

    row = {
        "row_id": "1",
        "order_id": "O-1",
        "order_date": "08/11/2017",
        "ship_date": "11/11/2017",
        "ship_mode": "Second Class",
        "customer_id": "C-1",
        "customer_name": "Claire Gute",
        "segment": "Consumer",
        "country": "United States",
        "city": "Henderson",
        "postal_code": "42420",
        "sales": "10.50",
        "order_year": None,
        "order_month": None,
        "order_day": None,
    }
    row.update(overrides)
    return row


@pytest.fixture
def silver_source_df(spark):
    """Build Spark DataFrames for Silver tests while allowing extra columns."""

    def factory(rows):
        columns = list(rows[0].keys())
        for row in rows[1:]:
            for column in row:
                if column not in columns:
                    columns.append(column)
        normalized_rows = [{column: row.get(column) for column in columns} for row in rows]
        schema = StructType([StructField(column, StringType(), True) for column in columns])
        return spark.createDataFrame(normalized_rows, schema=schema)

    return factory
