from abc import ABC, abstractmethod, abstractproperty

from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from gcp_airflow_foundations.base_class.source_table_config import SourceTableConfig


class DagBuilder(ABC):
    """
    A base DAG builder for creating a list of DAGs for a given source.

    Attributes:
        sources:              List of initialized subclasses of this class (dynamically updated during runtime)
        config:               DataSourceTablesConfig object for DAG configuration
        type_mappings:        Dictionary of source-specific type mappings between source and target (BigQuery)
        type:                 Ingestion type

    """
    sources = []
    config: DataSourceTablesConfig
    type_mappings: dict
    source_type: str

    def __init__(self, default_task_args: dict, config: DataSourceTablesConfig):
        self.default_task_args = default_task_args
        self.config = config
        self.validate_extra_options()
        super().__init__()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.sources.append(cls)

    @abstractmethod
    def build_dags(self):
        pass

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
