# -- @Rohesen Maurya

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,    # DDL + MERGE
    BigQueryCheckOperator,        # simple DQ gate (if available in your provider)
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# -----------------------
# Config
# -----------------------
PROJECT_ID = "horizontal-data-464415-v6"
DATASET_ID = "walmart_dwh"
LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"

BUCKET = "bigquery_projects123"
MERCHANTS_OBJ = "walmart_ingestion/merchants/merchants.json"
SALES_OBJ = "walmart_ingestion/sales/walmart_sales.json"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="walmart_sales_etl_gcs",
    default_args=default_args,
    description="ETL pipeline for Walmart sales data from GCS",
    schedule_interval="@daily",
    catchup=False,
    tags=["walmart", "bigquery", "gcs"],
) as dag:

    # 1) Dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        exists_ok=True,
    )

    # 2) Tables via DDL (partition + cluster without relying on operator kwargs)
    create_merchants_table = BigQueryInsertJobOperator(
        task_id="create_merchants_table",
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        configuration={
            "query": {
                "useLegacySql": False,
                "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.merchants_tb` (
                  merchant_id STRING NOT NULL,
                  merchant_name STRING,
                  merchant_category STRING,
                  merchant_country STRING,
                  last_update TIMESTAMP
                )
                -- optional clustering (wide scans benefit slightly)
                CLUSTER BY merchant_category, merchant_country;
                """,
            }
        },
    )

    create_walmart_sales_stage = BigQueryInsertJobOperator(
        task_id="create_walmart_sales_stage",
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        configuration={
            "query": {
                "useLegacySql": False,
                "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.walmart_sales_stage` (
                sale_id STRING NOT NULL,
                sale_date DATE,
                product_id STRING,
                quantity_sold INT64,
                total_sale_amount FLOAT64,
                merchant_id STRING,
                last_update TIMESTAMP
                )
                PARTITION BY sale_date
                CLUSTER BY merchant_id;
                """,
            }
        },
    )

    create_target_table = BigQueryInsertJobOperator(
    task_id="create_target_table",
    gcp_conn_id=GCP_CONN_ID,
    location=LOCATION,
    configuration={
        "query": {
            "useLegacySql": False,
            "query": f"""
            CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.walmart_sales_tgt` (
              sale_id STRING NOT NULL,
              sale_date DATE,
              product_id STRING,
              quantity_sold INT64,
              total_sale_amount FLOAT64,
              merchant_id STRING,
              merchant_name STRING,
              merchant_category STRING,
              merchant_country STRING,
              last_update TIMESTAMP
            )
            PARTITION BY sale_date
            CLUSTER BY merchant_id, product_id;
            """,
            }
        },
    )

    # 3) Load GCS → BigQuery (stage/dim)
    with TaskGroup(group_id="load_data") as load_data:
        gcs_to_bq_merchants = GCSToBigQueryOperator(
            task_id="gcs_to_bq_merchants",
            bucket=BUCKET,
            source_objects=[MERCHANTS_OBJ],
            destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.merchants_tb",
            write_disposition="WRITE_TRUNCATE",
            source_format="NEWLINE_DELIMITED_JSON",
            ignore_unknown_values=True,
            gcp_conn_id=GCP_CONN_ID,
            autodetect=True,
            location=LOCATION,
        )

        gcs_to_bq_walmart_sales = GCSToBigQueryOperator(
            task_id="gcs_to_bq_walmart_sales",
            bucket=BUCKET,
            source_objects=[SALES_OBJ],
            destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.walmart_sales_stage",
            write_disposition="WRITE_TRUNCATE",
            source_format="NEWLINE_DELIMITED_JSON",
            ignore_unknown_values=True,
            gcp_conn_id=GCP_CONN_ID,
            autodetect=True,
            location=LOCATION,
        )

    # 3.5) DQ check (fail if stage is empty)
    check_stage_has_rows = BigQueryCheckOperator(
        task_id="check_stage_has_rows",
        sql=f"SELECT COUNT(1) > 0 FROM `{PROJECT_ID}.{DATASET_ID}.walmart_sales_stage`",
        use_legacy_sql=False,
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
    )

    # 4) MERGE (UPSERT) dim+stage → fact/target
    merge_walmart_sales = BigQueryInsertJobOperator(
        task_id="merge_walmart_sales",
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        configuration={
            "query": {
                "useLegacySql": False,
                "query": f"""
                MERGE `{PROJECT_ID}.{DATASET_ID}.walmart_sales_tgt` T
                USING (
                  SELECT 
                    S.sale_id, 
                    S.sale_date, 
                    S.product_id, 
                    S.quantity_sold, 
                    S.total_sale_amount, 
                    S.merchant_id, 
                    M.merchant_name, 
                    M.merchant_category, 
                    M.merchant_country,
                    CURRENT_TIMESTAMP() AS last_update
                  FROM `{PROJECT_ID}.{DATASET_ID}.walmart_sales_stage` S
                  LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.merchants_tb` M
                  ON S.merchant_id = M.merchant_id
                ) S
                ON T.sale_id = S.sale_id
                WHEN MATCHED THEN UPDATE SET
                  sale_date = S.sale_date,
                  product_id = S.product_id,
                  quantity_sold = S.quantity_sold,
                  total_sale_amount = S.total_sale_amount,
                  merchant_id = S.merchant_id,
                  merchant_name = S.merchant_name,
                  merchant_category = S.merchant_category,
                  merchant_country = S.merchant_country,
                  last_update = S.last_update
                WHEN NOT MATCHED THEN INSERT (
                  sale_id, sale_date, product_id, quantity_sold, total_sale_amount,
                  merchant_id, merchant_name, merchant_category, merchant_country, last_update
                )
                VALUES (
                  S.sale_id, S.sale_date, S.product_id, S.quantity_sold, S.total_sale_amount,
                  S.merchant_id, S.merchant_name, S.merchant_category, S.merchant_country, S.last_update
                );
                """,
            }
        },
    )

    # Orchestration
    create_dataset >> [create_merchants_table, create_walmart_sales_stage, create_target_table] >> load_data
    load_data >> check_stage_has_rows >> merge_walmart_sales
