from abc import ABC, abstractmethod, abstractproperty

from airflow.models.dag import DAG

from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from gcp_airflow_foundations.base_class.source_table_config import SourceTableConfig
from gcp_airflow_foundations.base_class.source_config import SourceConfig
from gcp_airflow_foundations.enums.schema_source_type import SchemaSourceType
from gcp_airflow_foundations.common.gcp.load_builder import load_builder
from gcp_airflow_foundations.source_class.schema_source_config import AutoSchemaSourceConfig, GCSSchemaSourceConfig, BQLandingZoneSchemaSourceConfig

import logging

class DagBuilder(ABC):
    """A base DAG builder for creating a list of DAGs for a given source.

    Attributes
    ----------
    sources : list(DagBuilder)
        List of initialized subclasses of this class (dynamically updated during runtime)
    config : DataSourceTablesConfig
        DAG configuration object
    """
    sources = []

    def __init__(self, default_task_args: dict, config: DataSourceTablesConfig):
        self.default_task_args = default_task_args
        self.config = config
        self.validate_extra_options()
        self.set_schema_method_type()
        self.validate_custom_method()
        super().__init__()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, 'source_type'):
            raise AttributeError(f"{cls} source class is missing 'source_type' attribute")
        cls.sources.append(cls)

    def build_dags(self):
        """
        Main DAG building method
        """
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
                dag_id=f"{data_source.name}.{table_config.table_name}",
                description=f"{data_source.name} to BigQuery load for {table_config.table_name}",
                schedule_interval=data_source.ingest_schedule,
                default_args=table_default_task_args,
                render_template_as_native_obj=True
            ) as dag:    

                load_to_bq_landing = self.get_bq_ingestion_task(dag, table_config)

                self.get_datastore_ingestion_task(dag, load_to_bq_landing, data_source, table_config)
            
                dags.append(dag)
                
        dags = dags + self.get_extra_dags()
        return dags

    @abstractmethod
    def get_bq_ingestion_task(self, table_config: SourceTableConfig):
        """Abstract method for the Airflow task that ingests data to the BigQuery staging table"""
        pass

    def get_datastore_ingestion_task(self, dag, preceding_task, data_source: SourceConfig, table_config:SourceTableConfig):
        """Method for the Airflow task group that upserts data to the ODS and HDS tables"""
        load_builder(
            data_source=data_source,
            table_config=table_config,
            schema_config=BQLandingZoneSchemaSourceConfig,
            preceding_task=preceding_task,
            dag=dag
        )

    def get_extra_dags(self):
    # Override if non-table ingestion DAGs are needed for the flow: return them here
        return []

    @abstractmethod
    def set_schema_method_type(self):
        pass

    def get_schema_method_class(self):
        if self.schema_source_type == SchemaSourceType.GCS:
            return GCSSchemaSourceConfig

        elif self.schema_source_type == SchemaSourceType.AUTO:
            return AutoSchemaSourceConfig

        else:
            return

    def validate_custom_method(self):
        if self.schema_source_type== SchemaSourceType.CUSTOM:
            assert self.get_schema_method_class() is not None, 'A custom method for schema parsing must be provided'

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