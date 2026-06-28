# HEMA Retail Sales Data Pipeline

This repository contains a local-testable, AWS Glue-compatible medallion ETL pipeline for the HEMA data engineer technical assignment.

The pipeline reads the provided retail sales CSV, writes Bronze and Silver Parquet datasets, and publishes two Gold datasets:

- Gold Sales: one row per distinct order.
- Gold Customer: one row per customer with rolling and all-time order counts.

The proposed production design uses Amazon S3, AWS Glue, AWS Glue Data Catalog, AWS Lake Formation, EventBridge Scheduler, Step Functions, CloudWatch, CodePipeline, and CodeBuild. This repository does not deploy AWS infrastructure and intentionally does not include IaC or CI/CD configuration files.

## Repository Structure

- `data/train.csv`: source dataset used for local execution.
- `data/processed/`: generated example outputs from the local pipeline.
- `src/core.py`: reusable PySpark transformation, validation, IO, logging, and Glue Catalog helper logic.
- `glue_jobs/`: AWS Glue-compatible job entry points for Bronze, Silver, Gold Sales, and Gold Customer.
- `scripts/run_local_pipeline.py`: runs the full pipeline locally.
- `scripts/validate_outputs.py`: validates generated local outputs against expected dataset facts.
- `tests/`: PySpark unit and end-to-end tests.
- `Medallion_Architecture.md`: architecture explanation.
- `Medallion_Architecture.png`: high-level architecture diagram.

## Prerequisites

Use Python 3.10 or newer.

PySpark also needs a local Java runtime. Install Java 11 or Java 17 before running the pipeline or tests.

macOS example:

```bash
brew install openjdk@17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

Linux example:

```bash
sudo apt-get install openjdk-17-jdk
```

Check Java is available:

```bash
java -version
```

## Local Setup

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

If you do not want to create a virtual environment, the only required install command is:

```bash
python -m pip install -e ".[dev]"
```

## Run the Pipeline Locally

The default input is `data/train.csv`, and the default output root is `data/processed`.

```bash
python scripts/run_local_pipeline.py --pipeline_run_id local-001 --clean
```

The `--clean` flag removes the previous `data/processed` folder before writing a fresh run.

After the run, validate the outputs:

```bash
python scripts/validate_outputs.py
```

Expected validation facts for the provided dataset:

```text
bronze_rows: 9800
silver_valid_rows: 9800
silver_quarantine_rows: 0
gold_sales_rows: 4922
gold_customer_rows: 793
latest_order_date: 2018-12-30
last_month_distinct_orders: 234
last_six_month_distinct_orders: 1073
all_time_distinct_orders: 4922
```

Run the test suite:

```bash
pytest
```

Optional static check:

```bash
python -m ruff check .
```

## Output Locations

Parquet is the canonical medallion storage format.

- `data/processed/bronze`: normalized Bronze Parquet, partitioned by `order_year`, `order_month`, `order_day`.
- `data/processed/silver`: validated Silver Parquet, partitioned by `order_year`, `order_month`, `order_day`.
- `data/processed/quarantine/bronze_malformed_order_dates`: Bronze rows with unparseable order dates.
- `data/processed/quarantine/silver`: rejected Silver records with `_rejection_reason`.
- `data/processed/gold/sales`: Gold Sales Parquet, partitioned by order date.
- `data/processed/gold/customer`: Gold Customer snapshot Parquet, partitioned by snapshot date.
- `data/processed/gold_csv/sales`: headered CSV copy of Gold Sales for quick inspection.
- `data/processed/gold_csv/customer`: headered CSV copy of Gold Customer for quick inspection.

The CSV folders are reviewer-friendly copies generated from the same Gold DataFrames as the Parquet tables.

## Functional Mapping

Gold Sales contains:

- `order_id`
- `order_date`
- `shipment_date`
- `shipment_mode`
- `city`

Gold Customer contains:

- `customer_id`
- `customer_first_name`
- `customer_last_name`
- `customer_segment`
- `country`
- `orders_last_month`
- `orders_last_6_months`
- `orders_all_time`

Gold tables also include technical partition or snapshot columns in Parquet so the datasets remain query-efficient and traceable.

## AWS Glue Jobs

The `glue_jobs/` scripts are entry points that can be used as AWS Glue PySpark jobs. They use the same transformation code as the local runner.

Example job flow:

```text
bronze_job.py -> silver_job.py -> gold_sales_job.py
                               -> gold_customer_job.py
```

Typical Bronze job arguments:

```bash
--input_path s3://bucket/landing/train.csv
--output_path s3://bucket/bronze/retail_sales
--quarantine_path s3://bucket/quarantine/bronze_malformed_order_dates
--pipeline_run_id 2026-06-28
```

Typical Silver job arguments:

```bash
--input_path s3://bucket/bronze/retail_sales
--output_path s3://bucket/silver/retail_sales
--quarantine_path s3://bucket/quarantine/silver
--pipeline_run_id 2026-06-28
```

Typical Gold Sales job arguments:

```bash
--input_path s3://bucket/silver/retail_sales
--output_path s3://bucket/gold/sales
--pipeline_run_id 2026-06-28
```

Typical Gold Customer job arguments:

```bash
--input_path s3://bucket/silver/retail_sales
--output_path s3://bucket/gold/customer
--pipeline_run_id 2026-06-28
```

## AWS Glue Data Catalog

Catalog registration is optional and is not used by the local runner. In AWS Glue, pass `--catalog_database` and optionally `--catalog_table` to register a job output as an external Parquet table in the AWS Glue Data Catalog.

Example Gold Sales Catalog arguments:

```bash
--catalog_database retail_sales
--catalog_table retail_sales_gold_sales
```

When these arguments are provided, the job creates the database/table metadata and runs partition recovery after writing the Parquet output.

Default table names:

- Bronze: `retail_sales_bronze`
- Silver: `retail_sales_silver`
- Gold Sales: `retail_sales_gold_sales`
- Gold Customer: `retail_sales_gold_customer`

## Design Notes

Bronze preserves additive source attributes by normalizing all incoming column names instead of projecting a fixed select list. New source columns remain visible in Parquet and can be exposed through the Glue Data Catalog.

Silver validates the required business contract, casts known columns, trims strings, removes duplicate `row_id` records, and writes invalid rows to quarantine.

Gold exposes stable consumer contracts. New raw attributes are not automatically propagated to Gold unless the consumer contract is intentionally changed.

Gold Customer is a customer-level snapshot and does not have a single natural order date per row. It is partitioned by `snapshot_year`, `snapshot_month`, and `snapshot_day`, where the snapshot date is derived from the latest order date in Silver. For the provided dataset, this is `2018-12-30`.

Customer name parsing is deterministic: the first whitespace-delimited token becomes `customer_first_name`, and the remaining tokens become `customer_last_name`. In production, structured name fields should be supplied by the source domain.

## Troubleshooting

If PySpark fails with `JAVA_GATEWAY_EXITED` or `Unable to locate a Java Runtime`, install Java 11 or 17 and set `JAVA_HOME`.

If validation fails after changing transformation logic, rerun the pipeline with `--clean` before validating:

```bash
python scripts/run_local_pipeline.py --pipeline_run_id local-001 --clean
python scripts/validate_outputs.py
```
