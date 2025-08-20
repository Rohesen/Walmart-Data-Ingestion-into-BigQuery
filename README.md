# Walmart-Data-Ingestion-into-BigQuery
This project demonstrates how to build an ETL pipeline using Airflow to ingest Walmart data into Google BigQuery. The pipeline creates required dimension and staging tables, loads data from Google Cloud Storage (GCS), and performs an upsert  into the fact table using a BigQuery `MERGE` query.
