from dataclasses import fields
from urllib.parse import urlparse
import logging

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from gcp_airflow_foundations.base_class.salesforce_ingestion_config import SalesforceIngestionConfig

from gcp_airflow_foundations.operators.api.operators.sf_to_gcs_query_operator import SalesforceToGcsQueryOperator
from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.common.gcp.load_builder import load_builder


class SalesforcetoBQDagBuilder(DagBuilder):
    """
    Builds DAGs to load a CSV file from GCS to a BigQuery Table.
    """
    source_type = "SALESFORCE"

    def build_dags(self):
        data_source = self.config.source
        logging.info(f"Building DAG for GCS {data_source.name}")

        # GCP Parameters
        gcp_project = data_source.gcp_project
        gcs_bucket = data_source.extra_options["gcs_bucket"]
        gcs_object = data_source.extra_options["gcs_objects"]
        landing_dataset = data_source.landing_zone_options.landing_zone_dataset

        dags = []
        for table_config in self.config.tables:
            table_default_task_args = self.default_task_args_for_table(
                self.config, table_config
            )
            logging.info(f"table_default_task_args {table_default_task_args}")

            start_date = table_default_task_args["start_date"]

            with DAG(
                dag_id=f"sf_to_bq_{table_config.table_name}",
                description=f"Salesforce to BigQuery load for {table_config.table_name}",
                schedule_interval="@daily",
                default_args=table_default_task_args
            ) as dag:

                # BigQuery parameters
                destination_table = f"{gcp_project}:{landing_dataset}.{table_config.landing_zone_table_name_override}"

                # Salesforce parameters
                object_name = table_config.table_name
                ingest_all_columns = table_config.extra_options.get("ingest_all_columns")
                fields_to_omit = table_config.extra_options.get("fields_to_omit")
                field_names = table_config.extra_options.get("field_names")
                    
                # 1 Load Salesforce to CSV in GCS
                gcs_upload_task = SalesforceToGcsQueryOperator(
                    task_id="upload_sf_to_gcs",
                    salesforce_object=object_name,
                    ingest_all_fields=ingest_all_columns,
                    fields_to_omit=fields_to_omit,
                    fields_to_include=field_names,
                    include_deleted=False,
                    bucket_name=gcs_bucket,
                    object_name=gcs_object,
                    salesforce_conn_id="salesforce_default",
                    export_format='csv',
                    coerce_to_timestamp=False,
                    record_time_added=True,
                    gcp_conn_id="google_cloud_default",
                    dag=dag
                )

                # 2 Load CSV to BQ Landing Zone with auto-detect
                load_to_bq_landing = GCSToBigQueryOperator(
                    task_id='import_csv_to_bq_landing',
                    bucket=gcs_bucket,
                    source_objects=[gcs_object],
                    destination_project_dataset_table=destination_table,
                    write_disposition='WRITE_TRUNCATE',
                    create_disposition='CREATE_IF_NEEDED',
                    skip_leading_rows=1,
                    dag=dag
                )

                #3 Create task groups for loading data to ODS/HDS tables
                taskgroups = load_builder(
                    project_id=data_source.gcp_project,
                    table_id=table_config.table_name,
                    dataset_id=data_source.dataset_data_name,
                    landing_zone_dataset=landing_dataset,
                    landing_zone_table_name_override=table_config.landing_zone_table_name_override,
                    surrogate_keys=table_config.surrogate_keys,
                    column_mapping=table_config.column_mapping,
                    gcs_schema_object=table_config.source_table_schema_object,
                    schema_fields=None,
                    ods_table_config=table_config.ods_config,
                    hds_table_config=table_config.hds_config,
                    preceding_task=load_to_bq_landing,
                    dag=dag
                )

                gcs_upload_task >> load_to_bq_landing >> taskgroups

                logging.info(f"Created dag for {table_config}, {dag}")

                dags.append(dag)

        return dags

    def validate_extra_options(self):
        tables = self.config.tables

        for each table in tables:
            # either ingest all columns or one of field_names or fields_to_omit is non-empty
            sf_config = table.extra_options.get("sf_config")
            assert (sf_config.ingest_all_columns or (sf_config.field_names or sf_config.fields_to_omit))
            
