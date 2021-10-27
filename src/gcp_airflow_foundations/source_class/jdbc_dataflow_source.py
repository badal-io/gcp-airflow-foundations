from abc import ABC, abstractmethod, abstractproperty
import logging
from re import A
import re
from dacite import from_dict
from dataclasses import dataclass

from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from gcp_airflow_foundations.base_class.source_table_config import SourceTableConfig
from gcp_airflow_foundations.base_class.dataflow_job_config import DataflowJobConfig

from dataclasses import fields
from urllib.parse import urlparse
import logging

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.hooks.kms import CloudKMSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import Variable
from gcp_airflow_foundations.source_class.dataflow_taskgroups import dataflow_taskgroup_builder

from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.common.gcp.load_builder import load_builder


class JdbcToBQDataflowDagBuilder(DagBuilder):

    def build_dags(self):
        data_source = self.config.source
        logging.info(f"Building DAG for GCS {data_source.name}")

        # Source level Parameters
        system_name = data_source.extra_options["dataflow_job_config"]["system_name"]
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
                dag_id=f"{system_name}_to_bq_{table_config.table_name}",
                description=f"{system_name} to BigQuery load for {table_config.table_name}",
                schedule_interval="@daily",
                default_args=table_default_task_args
            ) as dag:

                # Table level parameters
                dataflow_job_params = data_source.extra_options["dataflow_job_config"]
                schema_table = dataflow_job_params["bq_schema_table"]
                dataflow_job_params["table_name"] = table_config.table_name
                destination_table = f"{gcp_project}:{landing_dataset}.{table_config.landing_zone_table_name_override}"
                destination_schema_table = f"{gcp_project}.{landing_dataset}.{schema_table}"

                taskgroup = dataflow_taskgroup_builder(dag,
                    query_schema=False,
                    dataflow_job_params=dataflow_job_params,
                    destination_table=destination_table,
                    destination_schema_table=destination_schema_table,
                    table_name=table_config.table_name,
                    system_name=system_name,
                    create_job_params=self.create_job_params,
                    run_dataflow_job=self.run_dataflow_job
                )

                dags.append(dag)

        sdag = self.get_schema_dag()
        dags.append(sdag)

        return dags

    def get_schema_dag(self):
        """
        This method returns a singular dag that runs a Dataflow job to fetch the global schemas from source.
        """
        data_source = self.config.source
        gcp_project = data_source.gcp_project
        landing_dataset = data_source.landing_zone_options.landing_zone_dataset

        dataflow_job_params = data_source.extra_options["dataflow_job_config"]

        schema_table_name = dataflow_job_params["bq_schema_table"]
        destination_table = f"{gcp_project}:{landing_dataset}.{schema_table_name}"
        system_name = dataflow_job_params["system_name"]

        with DAG(
            dag_id=f"{system_name}_upload_schema",
            description=f"Upload source schemas for all tables to BQ",
            schedule_interval="@daily",
            default_args=self.default_task_args_for_table(
                self.config, self.config.tables[0]
            )
        ) as schema_dag:

            taskgroup = dataflow_taskgroup_builder(
                schema_dag,
                query_schema=True,
                dataflow_job_params=dataflow_job_params,
                destination_table=destination_table,
                destination_schema_table=f"{gcp_project}.{landing_dataset}.{schema_table_name}",
                table_name=schema_table_name,
                system_name=system_name,
                create_job_params=self.create_job_params,
                run_dataflow_job=self.run_dataflow_job
            )

            return schema_dag

    def run_dataflow_job(self, template_path, system_name, table_name, query_schema, **kwargs):
        ti = kwargs['ti']
        if query_schema:
            xcom_task_pickup = "dataflow_taskgroup.create_job_parameters_schema"
        else:
            xcom_task_pickup = "dataflow_taskgroup.create_job_parameters"

        dataflow_default_options = ti.xcom_pull(key='dataflow_default_options', task_ids=xcom_task_pickup)
        parameters = ti.xcom_pull(key='parameters', task_ids=xcom_task_pickup)
        job_name = f"{system_name.lower()}-upload-{table_name.lower()}-to-bq".replace("_", "-")

        # hacky - TO FIX
        if isinstance(parameters["query"], list):
            parameters["query"] = parameters["query"][0]

        trigger_job = DataflowTemplatedJobStartOperator(
            task_id=job_name,
            job_name=job_name,
            template=template_path,
            dataflow_default_options=dataflow_default_options,
            parameters=parameters
        )
        trigger_job.execute(context=kwargs)

    @abstractmethod
    def create_job_params(self, config_params, destination_table, **kwargs):
        """
        Inputs:
        config_params:
            dictionary corresponding to a DataflowJobConfig
        destination_table:
            Bigquery table in form {gcp_project}.{bq_dataset}.{table_name}\
        query_schema:
            boolean - whether to query the schema or not

        When implemented, this method should create the following dictionaries, fill them 
        with the required parameters, and XCom push them

        DATAFLOW_DEFAULT_OPTIONS:
           project
           region
           subnetwork
        PARAMETERS:
           driverClassName
           driverJars
           KMSEncryptionKey 
           outputTable
           bigQueryLoadingTemporaryDirectory
           connectionURL 
           username
           password 
           query
        """
        pass

    def validate_extra_options(self):
        # try and parse as DataflowJobConfig
        job_cfg = from_dict(data_class=DataflowJobConfig, data=self.config.source.extra_options["dataflow_job_config"])
        
        # For tables - assert only FULL ingestion available for now   
        tables = self.config.tables
       # for table in tables:
       #     assert table.ingestion_type == "FULL"

            