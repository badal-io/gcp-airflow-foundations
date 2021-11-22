from dataclasses import fields
from urllib.parse import urlparse
import logging

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
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

    def set_schema_method_type(self):
        self.schema_source_type = self.config.source.schema_options.schema_source_type     

    def get_bq_ingestion_task(self, dag, table_config):
        data_source = self.config.source

        # GCP Parameters
        gcp_project = data_source.gcp_project
        gcs_bucket = data_source.extra_options["gcs_bucket"]
        landing_dataset = data_source.landing_zone_options.landing_zone_dataset 
        
        # BigQuery parameters
        destination_table = f"{gcp_project}:{landing_dataset}.{table_config.landing_zone_table_name_override}"

        # Salesforce parameters
        object_name = table_config.extra_options.get("sf_config")["api_table_name"]
        gcs_object = f"{object_name}.csv" 
        ingest_all_columns = table_config.extra_options.get("sf_config")["ingest_all_columns"]
        fields_to_omit = table_config.extra_options.get("sf_config")["fields_to_omit"]
        field_names = table_config.extra_options.get("sf_config")["field_names"]

        taskgroup = TaskGroup(group_id="salesforce_taskgroup")

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
            task_group=taskgroup
        )

        # 2 Load CSV to BQ Landing Zone with auto-detect
        load_to_bq_landing = PythonOperator(
            task_id='import_gcs_to_bq_landing',
            op_kwargs={"gcs_bucket": gcs_bucket,
                       "gcs_object": gcs_object,
                       "destination_table": destination_table},
            python_callable=self.gcs_to_bq,
            task_group=taskgroup
        )

        gcs_upload_task >> load_to_bq_landing

        return taskgroup

    def gcs_to_bq(self, gcs_bucket, gcs_object, destination_table, **kwargs):
        ds = kwargs["ds"]

        load_to_bq = GCSToBigQueryOperator(
            task_id='import_csv_to_bq_landing',
            bucket=gcs_bucket,
            source_objects=[gcs_object],
            destination_project_dataset_table=destination_table + f"_{ds}",
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=1,
        )
        load_to_bq.execute(context=kwargs)

    def validate_extra_options(self):
        tables = self.config.tables

        for table_config in tables:
            # either ingest all columns or one of field_names or fields_to_omit is non-empty
            sf_config = table_config.extra_options.get("sf_config")
            logging.info(sf_config)
            assert (sf_config["ingest_all_columns"] is True or (sf_config["field_names"] or sf_config["fields_to_omit"])), f"""Assertion error: ingest_all_columns is False and both field_names and fields_to_omit are empty for the object {table_config.table_name}"""
            
