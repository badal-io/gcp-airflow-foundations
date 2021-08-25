from abc import ABC, abstractmethod, abstractproperty

from airflow_framework.base_class.data_source_table_config import DataSourceTablesConfig
from airflow_framework.base_class.table_config import OdsTableConfig


class DagBuilder(ABC):
    """
    A base DAG buider
    """

    def __init__(self, default_task_args: dict):
        self.default_task_args = default_task_args
        super().__init__()

    @abstractmethod
    def build_dags(self, config: DataSourceTablesConfig):
        pass

    def default_task_args_for_table(
        self, config: DataSourceTablesConfig, table_config: OdsTableConfig
    ):
        return {
            **self.default_task_args,
            "start_date": config.table_start_date(table_config),
        }