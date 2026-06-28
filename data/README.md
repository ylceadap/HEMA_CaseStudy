# Data Outputs

This folder contains the provided source dataset and generated medallion outputs.

- `train.csv`: source CSV dataset.

- `processed/bronze`: normalized Bronze Parquet, partitioned by `order_year`, `order_month`, `order_day`.
- `processed/silver`: validated Silver Parquet, partitioned by `order_year`, `order_month`, `order_day`.
- `processed/quarantine`: rejected records. For the provided dataset, Silver quarantine is empty.
- `processed/gold/sales`: one row per order, partitioned by order date.
- `processed/gold/customer`: one row per customer snapshot, partitioned by snapshot date.
- `processed/gold_csv/sales.csv`: headered CSV copy of Gold Sales for quick inspection.
- `processed/gold_csv/customer.csv`: headered CSV copy of Gold Customer for quick inspection.

The Parquet folders are the canonical medallion outputs. The CSV files are reviewer-friendly copies generated from the same Gold DataFrames.

The outputs can be regenerated with:

```bash
python scripts/run_local_pipeline.py --pipeline_run_id local-validation --clean
python scripts/validate_outputs.py
```
