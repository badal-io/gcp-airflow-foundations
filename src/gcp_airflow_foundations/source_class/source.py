from abc import ABC, abstractmethod, abstractproperty

from airflow.models.dag import DAG

from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from gcp_airflow_foundations.base_class.source_table_config import SourceTableConfig

from gcp_airflow_foundations.enums.schema_source_type import SchemaSourceType
from gcp_airflow_foundations.common.gcp.source_schema.gcs import read_schema_from_gcs
from gcp_airflow_foundations.common.gcp.source_schema.bq import read_schema_from_bq

from gcp_airflow_foundations.common.gcp.load_builder import load_builder

import logging

class DagBuilder(ABC):
    """
    A base DAG builder for creating a list of DAGs for a given source.

    Attributes:
        sources:              List of initialized subclasses of this class (dynamically updated during runtime)
        config:               DataSourceTablesConfig object for DAG configuration

    """
    sources = []

    def __init__(self, default_task_args: dict, config: DataSourceTablesConfig):
        self.default_task_args = default_task_args
        self.config = config
        self.validate_extra_options()
        self.set_schema_method_type()
        self.schema_method = self.get_schema_method()
        self.validate_custom_method()
        super().__init__()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.sources.append(cls)

    def build_dags(self):
        data_source = self.config.source
        logging.info(f"Building DAG for {data_source.name}")

        dags = []
        for table_config in self.config.tables:
            table_default_task_args = self.default_task_args_for_table(
                self.config, table_config
            )
            logging.info(f"table_default_task_args {table_default_task_args}")

            start_date = table_default_task_args["start_date"]

            with DAG(
                dag_id=f"gcs_to_bq_{data_source.name}",
                description=f"BigQuery load for {table_config.table_name}",
                schedule_interval=None,
                default_args=table_default_task_args
            ) as dag:    

                load_to_bq_landing = self.get_bq_ingestion_task(table_config)
                load_to_bq_landing.dag = dag

                self.get_datastore_ingestion_task(dag, load_to_bq_landing, data_source, table_config)
            
            dags.append(dag)

        return dags

    @abstractmethod
    def get_bq_ingestion_task(self, table_config):
        pass

    def get_datastore_ingestion_task(self, dag, preceding_task, data_source, table_config):
        load_builder(
            project_id=data_source.gcp_project,
            table_id=table_config.table_name,
            dataset_id=data_source.dataset_data_name,
            landing_zone_dataset=data_source.landing_zone_options.landing_zone_dataset,
            landing_zone_table_name_override=table_config.landing_zone_table_name_override,
            surrogate_keys=table_config.surrogate_keys,
            column_mapping=table_config.column_mapping,
            schema_config=self.get_schema_method_args(table_config),
            ingestion_type=table_config.ingestion_type,
            partition_expiration=data_source.partition_expiration,
            ods_table_config=table_config.ods_config,
            hds_table_config=table_config.hds_config,
            preceding_task=preceding_task,
            dag=dag
        )

    @abstractmethod
    def set_schema_method_type(self):
        pass

    def get_schema_method(self):
        if self.schema_source_type == SchemaSourceType.GCS:
            return read_schema_from_gcs

        elif self.schema_source_type == SchemaSourceType.LANDINGZONE:
            return read_schema_from_bq

    def get_schema_method_args(self, table_config):
        if self.schema_source_type == SchemaSourceType.GCS:
            return {
                'method':self.schema_method,
                'kwargs':{'gcs_schema_object':self.config.source.schema_options.schema_object_template.format(
                    table_name=table_config.table_name
                )}
            }

        elif self.schema_source_type == SchemaSourceType.LANDINGZONE:  
            return {
                'method':self.schema_method,
                'kwargs':{'staging_dataset_id':self.config.source.landing_zone_options.landing_zone_dataset, 'staging_table_id':table_config.landing_zone_table_name_override}
            }

    def validate_custom_method(self):
        if self.set_schema_method_type() == SchemaSourceType.CUSTOM:
            assert self.get_schema_method() is not None, 'A custom method for schema parsing must be provided'

    @abstractmethod
    def validate_extra_options(self):
        pass

    def default_task_args_for_table(
        self, config: DataSourceTablesConfig, table_config: SourceTableConfig
    ):
        return {
            **self.default_task_args,
            "start_date": config.table_start_date(table_config)
        }
