from abc import ABC, abstractmethod

from gcp_airflow_foundations.common.gcp.source_schema.gcs import read_schema_from_gcs
from gcp_airflow_foundations.common.gcp.source_schema.bq import read_schema_from_bq


class SchemaSourceConfig(ABC):

    @abstractmethod
    def schema_method(self):
        pass

    @abstractmethod
    def schema_method_arguments(self, data_source, table_config, ds):
        pass

class AutoSchemaSourceConfig(SchemaSourceConfig):

    def schema_method(self):
        return

    def schema_method_arguments(self, data_source, table_config, ds):
        return


class GCSSchemaSourceConfig(SchemaSourceConfig):

    def schema_method(self):
        return read_schema_from_gcs

    def schema_method_arguments(self, data_source, table_config, ds):
        return {
                'gcs_schema_object':data_source.schema_options.schema_object_template.format(table_name=table_config.table_name)
            }


class BQLandingZoneSchemaSourceConfig(SchemaSourceConfig):

    def schema_method(self):
        return read_schema_from_bq

    def schema_method_arguments(self, data_source, table_config, ds):
        return {
                'dataset_id':data_source.landing_zone_options.landing_zone_dataset, 
                'table_id':f"{table_config.landing_zone_table_name_override}_{ds}"
            }
