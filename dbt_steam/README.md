# steam_dwh

Minimal dbt project for the local `MinIO -> Trino -> dbt` pipeline.

## Expected flow

1. Airflow writes NDJSON files into MinIO under `steam/landing/...`.
2. Trino exposes those paths as `hive.landing.*` external tables.
3. dbt reads `landing` sources and materializes `raw` tables plus `staging` views.

## Run order

1. Create the landing schemas and external tables from `../trino/sql/landing_tables.sql`.
2. Copy `profiles.yml.example` into your dbt profile location and adjust credentials if needed.
3. From this folder run:

```bash
dbt debug
dbt run
dbt test
```
