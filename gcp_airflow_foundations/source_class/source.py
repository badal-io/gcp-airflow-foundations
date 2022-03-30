from abc import ABC, abstractmethod, abstractproperty

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
import copy

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

        table_names_to_ingest = [self.config.tables[i].table_name for i in range(len(self.config.tables))]
        templated_config = False
        if data_source.regex_matching:
            full_ingestion_options = data_source.full_ingestion_options
            if full_ingestion_options.dag_creation_mode == "SOURCE":
                return self.build_dags_source_level()
            else:
                table_names_to_ingest = self.get_source_tables_to_ingest()
                templated_table_config = self.config.tables[0]
                templated_config = True

        dags = []
        
        for i, table_name in enumerate(table_names_to_ingest):
            table_default_task_args = self.default_task_args_for_table(
                self.config, table_config
            )
            # two possibilities: if we have a list of tables + one templated config, or a list of tables each with their own
            if templated_config:
                table_config = templated_table_config
                table_config.table_name = table_name
            else:
                table_config = self.config.tables[i]

            logging.info(f"table_default_task_args {table_default_task_args}")

            start_date = table_default_task_args["start_date"]
            
            kwargs = data_source.dag_args if data_source.dag_args else {}
            
            with DAG(
                dag_id=f"{data_source.name}.{table_config.table_name}",
                description=f"{data_source.name} to BigQuery load for {table_config.table_name}",
                schedule_interval=data_source.ingest_schedule,
                default_args=table_default_task_args,
                render_template_as_native_obj=True,
                **kwargs
            ) as dag:    

                self.set_task_dependencies(dag, table_config, data_source, "schema_parsing")
                dags.append(dag)

        extra_dags=  self.get_extra_dags()

        if extra_dags is not None and not extra_dags == []:
            dags = dags + self.get_extra_dags()

        return dags

    def build_dags_source_level(self):
        """
        Build one DAG per source for regex matching or full source ingestion 
        """
        data_source = self.config.source

        logging.info(f"Building DAG for {data_source.name}")

        dags = []
        tables = self.get_source_tables_to_ingest()

        # one template table config is filled out for the entire source
        templated_table_config = self.config.tables[0]
        table_default_task_args = self.default_task_args_for_table(
            self.config, templated_table_config
        )
        logging.info(f"table_default_task_args {table_default_task_args}")

        start_date = table_default_task_args["start_date"]
        
        kwargs = data_source.dag_args if data_source.dag_args else {}
        
        with DAG(
            dag_id=f"{data_source.name}.{data_source.full_ingestion_options.ingestion_name}",
            description=f"{data_source.name} to BigQuery load",
            schedule_interval=data_source.ingest_schedule,
            default_args=table_default_task_args,
            render_template_as_native_obj=True,
            **kwargs
        ) as dag:    
            
            for table_name in tables:
                table_config = templated_table_config
                table_config.table_name = table_name

                schema_parsing_task_id = "schema_parsing"
                if data_source.full_ingestion_options.dag_creation_mode == "SOURCE":
                    schema_parsing_task_id = f"{table_config.table_name}.{schema_parsing_task_id}"
                
                with TaskGroup(group_id=table_config.table_name) as table_task_group:
                    self.set_task_dependencies(dag, table_config, data_source, schema_parsing_task_id)
                table_task_group
            
            dags.append(dag)
        extra_dags=  self.get_extra_dags()

        if extra_dags is not None and not extra_dags == []:
            dags = dags + self.get_extra_dags()

        return dags

    def set_task_dependencies(self, dag: DAG, table_config: SourceTableConfig, data_source: SourceConfig, schema_parsing_task_id: str):
        load_to_bq_landing = self.get_bq_ingestion_task(dag, table_config)
        self.get_datastore_ingestion_task(dag, load_to_bq_landing, data_source, table_config, schema_parsing_task_id)

    @abstractmethod
    def get_bq_ingestion_task(self, table_config: SourceTableConfig):
        """Abstract method for the Airflow task that ingests data to the BigQuery staging table"""
        pass

    def get_datastore_ingestion_task(self, dag, preceding_task, data_source: SourceConfig, table_config:SourceTableConfig, schema_parsing_task_id: str):
        """Method for the Airflow task group that upserts data to the ODS and HDS tables"""
        load_builder(
            data_source=data_source,
            table_config=table_config,
            schema_config=BQLandingZoneSchemaSourceConfig,
            preceding_task=preceding_task,
            schema_parsing_task_id=schema_parsing_task_id,
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