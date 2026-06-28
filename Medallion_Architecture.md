# AWS Architecture

This solution is designed as a daily AWS batch pipeline following a Bronze, Silver, and Gold medallion architecture. The main goal is to turn the source retail sales CSV into reliable, governed datasets that can be discovered and queried by downstream users.

The reusable PySpark transformation logic is implemented in `src/core.py`, while the executable AWS Glue entry points are stored in `glue_jobs/`. The AWS orchestration, governance, observability, and CI/CD components shown below represent the proposed production design. They are not deployed by this repository.

## Data Flow

1. Source CSV files are placed in an immutable Amazon S3 landing area.
2. Amazon EventBridge Scheduler triggers the workflow once per day.
3. AWS Step Functions starts the Bronze Glue job.
4. The Bronze job:
   - converts source column names to lower snake_case;
   - parses the order and shipment dates;
   - preserves all source attributes, including newly added columns;
   - adds lineage fields such as the source file, ingestion timestamp, and pipeline run ID;
   - writes Parquet files partitioned by `order_year`, `order_month`, and `order_day`.
5. Malformed source records are written to the Bronze quarantine area.
6. After Bronze succeeds, Step Functions starts the Silver Glue job.
7. The Silver job:
   - validates the required business columns;
   - casts known fields to their expected types;
   - trims string values;
   - deduplicates records using `row_id`;
   - separates valid and rejected records;
   - writes valid Parquet data partitioned by order date.
8. Invalid records and duplicate rows are written to the Silver quarantine area with an `_rejection_reason`.
9. After Silver succeeds, Step Functions starts the Gold Sales and Gold Customer jobs in parallel.
10. Gold Sales produces one row per order.
11. Gold Customer produces one row per customer, including rolling distinct-order metrics.
12. The Gold tables are registered in the AWS Glue Data Catalog.
13. AWS Lake Formation governs access to the cataloged data.
14. Amazon Athena provides an example query interface for data analysts and other downstream users.

## Medallion Layers

### Landing / Raw

The landing area stores the original source CSV without modification. Keeping the source file unchanged makes it possible to trace issues back to the original input and rerun the pipeline when necessary.

### Bronze

The Bronze layer keeps the source data at its original row-level grain while introducing a consistent technical structure. It normalizes column names, parses dates, adds lineage metadata, and stores the result as partitioned Parquet.

The layer is intentionally permissive toward additive schema changes. If a new source attribute appears, it is preserved rather than silently dropped.

### Silver

The Silver layer contains cleaned and validated retail sales records. It applies the known schema, trims values, checks the required fields, removes duplicate `row_id` records, and separates invalid records into quarantine.

This layer is the trusted input for both Gold datasets.

### Gold

The Gold layer contains consumer-facing datasets with stable contracts:

- **Gold Sales** contains one row per distinct order.
- **Gold Customer** contains one row per customer with customer-level order metrics.

The Gold schemas are intentionally controlled. New source columns do not automatically appear in these datasets, because doing so could unexpectedly affect downstream consumers.

## Orchestration

Amazon EventBridge Scheduler provides the daily trigger, while AWS Step Functions controls the workflow.

The execution order is:

Bronze → Silver → Gold Sales and Gold Customer in parallel

## CI/CD Design

The CI/CD pipeline is used to test and release changes to the ETL code and workflow configuration. It is separate from the daily data pipeline itself.

The proposed flow is:

Code is pushed to GitHub or GitLab
A developer updates one of the ETL jobs, for example the Silver validation logic or the Gold Customer aggregation, and pushes the change to the shared repository.
AWS CodePipeline starts automatically
CodePipeline detects the new commit and starts the release process. It coordinates the different CI/CD stages.

AWS CodeBuild tests and packages the code
CodeBuild installs the project dependencies and runs:

linting;
unit tests;
PySpark transformation tests;
packaging of the Glue job scripts.

If any test fails, the pipeline stops and the code is not deployed.

The tested version is deployed to Development
The Glue job scripts and related workflow artifacts are first released to a development environment.

This allows the team to check that:

the jobs start correctly;
the expected input and output paths are used;
the transformations still produce the correct datasets;
no existing downstream contract is broken.
A manual approval is required
Once the development version has been checked, an authorized team member reviews the release and approves promotion to production.
The approved version is deployed to Production
The production Glue job scripts and operational configuration are updated with the tested and approved version.

The CI/CD pipeline therefore controls how code changes are tested and released.

It does not process the daily retail sales data. The daily data workflow is handled separately by:

EventBridge Scheduler
→ Step Functions
→ Glue Bronze
→ Glue Silver
→ Glue Gold jobs

For example, if a developer changes the Silver rule for invalid shipment dates, CI/CD tests and deploys that code change. After deployment, the daily EventBridge and Step Functions workflow runs the new version against the incoming sales data.

In line with the assignment requirements, this repository only describes the CI/CD design. It does not include GitHub Actions, CodePipeline configuration, buildspec.yml, Terraform, CloudFormation, or CDK files.