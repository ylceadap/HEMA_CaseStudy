# HEMA Retail Sales Data Pipeline

## 1. Assignment Goal

This project implements the ETL part and the AWS design part of the HEMA data engineer technical assignment.

The business goal is to take the provided retail sales CSV dataset, process it through a reliable data pipeline, and publish curated datasets that downstream users can discover and query. The final outputs are:

- **Gold Sales**: one row per distinct order.
- **Gold Customer**: one row per customer, including order counts for the last month, last six months, and all time.

The technical goal is to show a production-oriented data engineering approach:

- daily batch orchestration;
- medallion architecture with Bronze, Silver, and Gold layers;
- data quality checks and quarantine outputs;
- traceability through lineage fields and structured logging;
- schema evolution support for new source attributes;
- AWS Glue-compatible ETL jobs;
- optional publication to the AWS Glue Data Catalog.

No AWS infrastructure is deployed by this repository, and no IaC or CI/CD configuration files are included, in line with the assignment requirement.

## 2. Architecture Overview

The proposed production architecture uses AWS managed services around the PySpark ETL code in this repository.

High-level flow:

```text
Landing CSV in S3
    -> EventBridge Scheduler
    -> Step Functions
    -> Glue Bronze Job
    -> Glue Silver Job
        |-> Glue Gold Sales Job
        `-> Glue Gold Customer Job
    -> Glue Data Catalog
    -> Lake Formation / Athena / downstream consumers
```

The same transformation logic can also run locally through `scripts/run_local_pipeline.py`.

### AWS Tools Used

- **Amazon S3**: stores landing, Bronze, Silver, Gold, and quarantine data.
- **AWS Glue PySpark Jobs**: run the ETL transformations.
- **AWS Glue Data Catalog**: stores external table metadata for selected Parquet outputs.
- **AWS Lake Formation**: governs access to the cataloged datasets.
- **Amazon EventBridge Scheduler**: triggers the workflow once per day.
- **AWS Step Functions**: orchestrates job dependencies.
- **Amazon CloudWatch Logs**: stores structured job logs.
- **AWS CodePipeline / CodeBuild**: proposed CI/CD design for testing and deploying ETL code, without including CI/CD files in this repository.

The AWS architecture diagram is included in [Medallion_Architecture.png](Medallion_Architecture.png), with supporting notes in [Medallion_Architecture.md](Medallion_Architecture.md).

## 3. Key Design Decisions

I treated the source file as product-line-level data, not order-level data. The same `order_id` can appear on multiple rows when one order contains multiple products, so Gold Sales aggregates to one row per distinct order, and Gold Customer counts distinct `order_id` values instead of raw rows.

I use the latest available `order_date` in Silver as the snapshot date for rolling customer metrics. For this dataset that date is `2018-12-30`, but the code derives it dynamically instead of hardcoding it.

Bronze is permissive toward additive schema changes: if a new column such as `Promotion Code` appears, Bronze normalizes it to `promotion_code` and keeps it. Gold is intentionally more controlled, because downstream users need stable business-facing schemas.

The provided dataset is clean for the main ID and date checks: there are no empty required IDs and no duplicated `Row ID` values. I still included quarantine and deduplication logic so the pipeline can handle those issues when future daily files are less clean.

## 4. Medallion Architecture

This project follows a medallion architecture: each layer has a different responsibility and a different level of trust.

```text
Landing CSV
   -> Bronze: standardized raw-like data
   -> Silver: validated and cleaned data
   -> Gold: business-ready consumer datasets
```

### Bronze Layer

Bronze keeps the source data close to the original shape, but makes it easier to process and trace.

Main actions:

- reads the source CSV;
- normalizes all column names to lower snake_case;
- preserves original date values in `_raw_order_date` and `_raw_ship_date`;
- parses `order_date` and `ship_date` into Spark date columns;
- preserves all source attributes, including newly added columns;
- adds lineage fields:
  - `_ingestion_timestamp`
  - `_source_file`
  - `_pipeline_run_id`
- writes Parquet partitioned by:
  - `order_year`
  - `order_month`
  - `order_day`
- sends rows with unparseable `order_date` to Bronze quarantine.

Small example:

```text
Input column:  Order Date
Bronze columns: _raw_order_date = "08/11/2017", order_date = 2017-11-08

Input column:  Sub-Category
Bronze column: sub_category

Input column:  Promotion Code
Bronze column: promotion_code
```

The last example matters for schema evolution. If the source dataset receives a new column such as `Promotion Code`, Bronze does not drop it. It normalizes the name and keeps the column in Parquet.

Implementation:

- `normalize_column_name()`
- `normalize_columns()`
- `transform_bronze()`
- `split_bronze_by_order_date()`

These are implemented in [src/core.py](src/core.py).

### Silver Layer

Silver is the trusted cleaned layer. It applies the business data-quality contract before data can feed Gold.

Main actions:

- checks that required business columns exist;
- trims leading and trailing spaces from string fields;
- casts important fields to correct types:
  - `row_id` -> long
  - `order_date` -> date
  - `ship_date` -> date
  - `sales` -> double
- deduplicates records by `row_id`;
- validates required values and business rules;
- writes rejected records to quarantine with `_rejection_reason`;
- keeps additive new columns when they do not violate validation.

Small example for string trim:

```text
Input city:  "  Amsterdam  "
Silver city: "Amsterdam"
```

Small example for type cast:

```text
Input sales:  "10.50"  as string
Silver sales: 10.50    as double
```

Small example for quarantine:

```text
Input row:
order_id = ""

Silver result:
row is rejected with _rejection_reason = "order_id is null"
```

Validation rules include:

- `row_id` must not be null;
- `order_id` must not be null or blank;
- `order_date` must be parseable;
- `ship_date` must be parseable;
- `customer_id` must not be null or blank;
- `customer_name` must not be null or blank;
- `ship_date` must not be earlier than `order_date`;
- `sales` must be greater than zero;
- duplicate `row_id` records are rejected after deterministic ranking.

Not every nullable field is rejected. For example, `postal_code` can be null because it is not required for the Gold outputs.

The code is able to handle missing required IDs and duplicated `row_id` values, but the provided dataset is clean for these checks: it has no empty `Row ID`, `Order ID`, `Customer ID`, or `Customer Name`, and it has no duplicated `Row ID`. Therefore, the sample run has `silver_quarantine_rows: 0`.

Implementation:

- `prepare_silver()`
- `split_silver_valid_quarantine()`

These are implemented in [src/core.py](src/core.py).

### Gold Layer

Gold contains stable, business-facing datasets for downstream users.

There are two Gold outputs because the assignment requires the Bronze dataset to be split into Sales and Customer datasets.

#### Gold Sales

Gold Sales produces one row per distinct order.

Required business columns:

- `order_id`
- `order_date`
- `shipment_date`
- `shipment_mode`
- `city`

Small example:

```text
Silver has multiple rows for order CA-2018-100111 because the order contains multiple products.

Gold Sales keeps one row:
order_id = CA-2018-100111
order_date = 2018-12-30
shipment_date = 2019-01-05
shipment_mode = Standard Class
city = New York City
```

Before collapsing line items to one order row, the code checks that order-level attributes do not conflict within the same `order_id`.

Implementation:

- `validate_order_level_consistency()`
- `create_gold_sales()`

#### Gold Customer

Gold Customer produces one row per customer.

Required business columns:

- `customer_id`
- `customer_first_name`
- `customer_last_name`
- `customer_segment`
- `country`
- `orders_last_month`
- `orders_last_6_months`
- `orders_all_time`

Small example:

```text
customer_name = "Darrin Van Huff"

customer_first_name = "Darrin"
customer_last_name = "Van Huff"
```

Order counts are based on distinct `order_id`, not raw line-item rows. This avoids counting one multi-product order multiple times.

Gold Customer assumes one stable customer identity record per `customer_id`. Before aggregation, it checks that `customer_name`, `segment`, and `country` do not conflict within the same customer.

The rolling windows end at the latest order date in the dataset. For the provided data, the latest order date is:

```text
2018-12-30
```

So:

- `orders_last_month` counts distinct orders from one month before `2018-12-30`;
- `orders_last_6_months` counts distinct orders from six months before `2018-12-30`;
- `orders_all_time` counts all distinct orders for that customer.

Gold Customer is partitioned by snapshot date rather than order date because a customer-level row does not have one natural order date. The snapshot date is derived from `max(order_date)` in Silver.

Implementation:

- `validate_customer_level_consistency()`
- `create_gold_customer()`

Gold functions are implemented in [src/core.py](src/core.py).

## 5. Files and Folders

- `README.md`: this project guide and reproduction instructions.
- `Medallion_Architecture.md`: detailed AWS architecture explanation.
- `Medallion_Architecture.png`: high-level architecture diagram.
- `pyproject.toml`: package metadata, dependencies, pytest config, and ruff config.
- `src/core.py`: reusable transformation, validation, IO, logging, and Glue Catalog logic.
- `glue_jobs/bronze_job.py`: AWS Glue-compatible Bronze ingestion entry point.
- `glue_jobs/silver_job.py`: AWS Glue-compatible Silver cleansing entry point.
- `glue_jobs/gold_sales_job.py`: AWS Glue-compatible Gold Sales entry point.
- `glue_jobs/gold_customer_job.py`: AWS Glue-compatible Gold Customer entry point.
- `scripts/run_local_pipeline.py`: local end-to-end runner.
- `scripts/validate_outputs.py`: output validation script for the provided dataset.
- `tests/`: unit and end-to-end tests for Bronze, Silver, Gold Sales, and Gold Customer logic.
- `data/train.csv`: source dataset used for local execution.
- `data/processed/`: generated sample outputs from the local pipeline.
- `data/processed/gold_csv/`: reviewer-friendly CSV copies of Gold outputs.

## 6. Output Locations

Parquet is the canonical medallion storage format.

- `data/processed/bronze`: Bronze Parquet, partitioned by order date.
- `data/processed/silver`: Silver Parquet, partitioned by order date.
- `data/processed/quarantine/bronze_malformed_order_dates`: Bronze rejected rows.
- `data/processed/quarantine/silver`: Silver rejected rows.
- `data/processed/gold/sales`: Gold Sales Parquet.
- `data/processed/gold/customer`: Gold Customer snapshot Parquet.
- `data/processed/gold_csv/sales`: CSV copy of Gold Sales.
- `data/processed/gold_csv/customer`: CSV copy of Gold Customer.

Spark `_SUCCESS` files and CRC files are ignored by git.

## 7. Environment Requirements

Use Python 3.10 or newer.

PySpark also needs Java. Install Java 11 or Java 17 before running the pipeline or tests.

macOS example:

```bash
brew install openjdk@17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

Linux example:

```bash
sudo apt-get install openjdk-17-jdk
```

Check Java:

```bash
java -version
```

## 8. Local Setup

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

If a virtual environment is not needed, install directly:

```bash
python -m pip install -e ".[dev]"
```

## 9. Run Locally

### Processing Assumption

The local implementation treats the provided CSV as a complete snapshot and rebuilds the Bronze, Silver, and Gold outputs using overwrite mode.

In a production daily incremental pipeline, Bronze and Silver would normally use an append or partition-overwrite strategy with idempotent processing. Gold Customer would still require access to the complete Silver history, or an incremental state-management approach, to calculate rolling and all-time order metrics correctly.

Run the full local pipeline:

```bash
python scripts/run_local_pipeline.py --pipeline_run_id local-001 --clean
```

The default input is:

```text
data/train.csv
```

The default output root is:

```text
data/processed
```

The `--clean` flag removes the previous output folder before writing a fresh run.

Validate the generated outputs:

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

Run tests:

```bash
pytest
```

Run static checks:

```bash
python -m ruff check .
```

## 10. AWS Glue Job Usage

The `glue_jobs/` scripts can be uploaded as AWS Glue PySpark jobs. They use the same logic as the local runner.

Expected job order:

```text
bronze_job.py
  -> silver_job.py
      |-> gold_sales_job.py
      `-> gold_customer_job.py
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

## 11. AWS Glue Data Catalog

Catalog registration is optional. Local execution does not need AWS credentials because it writes local Parquet and CSV outputs only.

In AWS, S3 stores the actual data files, while the AWS Glue Data Catalog stores table metadata such as schema, location, and partitions. Lake Formation can then govern access to those cataloged tables, and Athena can query them.

In AWS Glue jobs, pass `--catalog_database` and optionally `--catalog_table` to create an external Parquet table entry for an output.

Example:

```bash
--catalog_database retail_sales
--catalog_table retail_sales_gold_sales
```

When these arguments are provided, the job:

- creates the database if needed;
- creates the external Parquet table if it does not already exist;
- runs `MSCK REPAIR TABLE` after writing the output so partitions are discovered.

The helper is intentionally simple. It does not manage the full schema lifecycle for existing tables and does not automatically update an existing table definition when the schema changes.

Default table names:

- Bronze: `retail_sales_bronze`
- Silver: `retail_sales_silver`
- Gold Sales: `retail_sales_gold_sales`
- Gold Customer: `retail_sales_gold_customer`

## 12. Schema Evolution Strategy

The assignment states that the source dataset may evolve with new attributes. This project handles that as follows:

- Bronze preserves all source columns after normalizing names.
- Silver validates a minimum required contract but keeps additive columns.
- Gold exposes stable business contracts and only adds new fields intentionally.
- Optional Glue Data Catalog registration makes selected Parquet outputs queryable as external tables.

Example:

```text
New source field: Promotion Code
Bronze output:    promotion_code is preserved
Silver output:    promotion_code is preserved if validation passes
Gold output:      unchanged until the business contract is intentionally updated
```

This balances transparent schema discovery with stable curated datasets.

## 13. Logging and Traceability

The pipeline uses structured JSON logs through `configure_logger()` in [src/core.py](src/core.py).

Logs are written to stdout. Locally, they appear in the terminal. In AWS Glue, stdout and stderr are captured by CloudWatch Logs. The project does not write local log files.

Every job log includes:

- `job_name`
- `pipeline_run_id`
- input/output paths;
- row counts;
- rejection counts where applicable;
- event names such as `job_start`, `input_read`, `data_transformed`, `output_written`, `catalog_registered`, `job_end`, and `job_failed`;
- exception stack traces on failure.

Bronze also adds row-level lineage columns:

- `_ingestion_timestamp`
- `_source_file`
- `_pipeline_run_id`

These fields make it possible to trace when a row was ingested, from which source file, and in which pipeline run.

## 14. Troubleshooting

If PySpark fails with `JAVA_GATEWAY_EXITED` or `Unable to locate a Java Runtime`, install Java 11 or 17 and set `JAVA_HOME`.

If validation fails after changing transformation logic, rerun the pipeline with `--clean` before validating:

```bash
python scripts/run_local_pipeline.py --pipeline_run_id local-001 --clean
python scripts/validate_outputs.py
```
