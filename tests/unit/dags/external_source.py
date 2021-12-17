import logging

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator

from gcp_airflow_foundations.source_class.source import DagBuilder


class CustomDagBuilder(DagBuilder):
    
    source_type = "CUSTOM"

    def set_schema_method_type(self):
        self.schema_source_type = self.config.source.schema_options.schema_source_type

    def get_bq_ingestion_task(self, dag, table_config):

        return DummyOperator(task_id='dummy', dag=dag)
         
    def validate_extra_options(self):
        pass