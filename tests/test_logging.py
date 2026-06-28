"""Tests for structured logging behavior that does not require Spark."""

from __future__ import annotations

import json
import logging
from io import StringIO

import pytest

from core import configure_logger


@pytest.fixture
def isolated_root_logger():
    """Temporarily isolate root logger handlers for deterministic stdout tests."""

    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    stream = StringIO()
    root.handlers.clear()
    root.addHandler(logging.StreamHandler(stream))
    try:
        yield root, stream
    finally:
        root.handlers.clear()
        root.handlers.extend(original_handlers)
        root.setLevel(original_level)


def _read_single_log_line(stream: StringIO) -> dict[str, object]:
    lines = [line for line in stream.getvalue().splitlines() if line.strip()]
    assert len(lines) == 1
    return json.loads(lines[0])


def test_json_logs_include_job_name_and_pipeline_run_id(isolated_root_logger):
    _, stream = isolated_root_logger
    logger = configure_logger("silver_cleansing", "run-123")

    logger.info("job_start", status="started")

    payload = _read_single_log_line(stream)
    assert payload["message"] == "job_start"
    assert payload["job_name"] == "silver_cleansing"
    assert payload["pipeline_run_id"] == "run-123"
    assert payload["status"] == "started"


def test_bound_context_is_included(isolated_root_logger):
    _, stream = isolated_root_logger
    logger = configure_logger("bronze_ingestion", "run-456").bind(
        input_path="s3://bucket/landing/train.csv",
        output_path="s3://bucket/bronze/retail_sales",
    )

    logger.info("input_read", input_row_count=9800)

    payload = _read_single_log_line(stream)
    assert payload["message"] == "input_read"
    assert payload["input_path"] == "s3://bucket/landing/train.csv"
    assert payload["output_path"] == "s3://bucket/bronze/retail_sales"
    assert payload["input_row_count"] == 9800


def test_exception_logs_include_stack_trace(isolated_root_logger):
    _, stream = isolated_root_logger
    logger = configure_logger("gold_sales", "run-789")

    try:
        raise ValueError("example failure")
    except ValueError:
        logger.exception("job_failed", status="failed")

    payload = _read_single_log_line(stream)
    assert payload["message"] == "job_failed"
    assert payload["status"] == "failed"
    assert "stack_trace" in payload
    assert "ValueError: example failure" in str(payload["stack_trace"])


def test_repeated_logger_configuration_does_not_duplicate_handlers(isolated_root_logger):
    root, stream = isolated_root_logger
    configure_logger("gold_customer", "run-001")
    handler_count = len(root.handlers)
    logger = configure_logger("gold_customer", "run-001")

    logger.info("job_end", status="succeeded")

    assert len(root.handlers) == handler_count
    payload = _read_single_log_line(stream)
    assert payload["message"] == "job_end"
