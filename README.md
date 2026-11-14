
# Bank Churn Lakehouse (Databricks, PySpark, Delta Lake)

A data pipeline built on Databricks Free Edition with PySpark and Delta Lake. It uses the public Kaggle bank customer churn dataset (10,000 customers, 18 columns) and follows a bronze / silver / gold structure.

> The Kaggle file is a single snapshot, so the "day 2" batch (changed customers, new customers, bad rows) is **simulated** to test the pipeline.

## What the pipeline does

**Bronze** (`bronze_churn`)
- Reads the raw CSV and finds stray empty columns in the file (13 extra columns, plus a few stray text values). Checks them, then drops them.
- Renames columns to lowercase with underscores (Delta does not allow spaces in names).
- Adds `_ingested_at` and `_source_file` columns so every row can be traced.
- Saves 10,000 rows as a Delta table.

**Silver** (`silver_churn`, `silver_churn_rejects`)
- Checks the data first (row count, distinct customers, min/max values).
- Applies 6 data quality rules (customer ID present, credit score range, age range, valid country, non-negative balance, valid exit flag).
- A reusable function splits rows into valid and rejected, and each rejected row records which rule it broke.
- Tested the rules on deliberately broken rows (age 7, balance -100, country "Italy"). All were caught.
- Uses a Delta `MERGE` to update existing customers and insert new ones. Re-running it does not create duplicates (silver stayed at 10,100 rows).

**History table** (`dim_customer_scd2`)
- Slowly Changing Dimension (Type 2) for country, number of products, active flag and card flag.
- A hash of those values detects changes. One `MERGE` closes the old version and inserts the new one.
- With the simulated day 2 data: 500 customers changed country, giving 10,100 current rows and 500 closed rows.

**Gold**
- `gold_churn_by_geography`: churn rate and lost balance per country
- `gold_churn_by_age_band`: churn rate per age group
- `gold_at_risk_balance`: customers who are inactive with one product and have not left yet, and their total balance (a simple rule, not a prediction model)

## Tech
Databricks Free Edition (serverless), PySpark, Delta Lake, SQL

## Files
- `churn_lakehouse_pipeline.py`: the full pipeline as a Databricks notebook

## Data
Kaggle "Bank Customer Churn" dataset (`Customer-Churn-Records.csv`). Not included in this repo.

## Possible next steps
Incremental ingestion with Auto Loader, a scheduled Databricks Workflow, and automated tests.
