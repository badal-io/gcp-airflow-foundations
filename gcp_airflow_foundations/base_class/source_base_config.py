from airflow.exceptions import AirflowException

from dataclasses import dataclass
from dacite import from_dict

from pydantic import root_validator

from regex import E

from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.base_class.column_udf_config import ColumnUDFConfig


@dataclass
class SourceBaseConfig:
    """
    Base table/template configuration data class.
    """

    @root_validator(pre=True)
    def valid_hds_dataset(cls, values):
        if (values["cluster_fields"] is not None) and (
            values["column_mapping"] is not None
        ):
            values["cluster_fields"] = [
                values["column_mapping"].get(field, field)
                for field in values["cluster_fields"]
            ]
        return values

    @root_validator(pre=True)
    def valid_column_casting(cls, values):
        if values["column_casting"] is not None:
            assert all(
                [
                    key not in values["surrogate_keys"]
                    for key in values["column_casting"]
                ]
            ), "Column casting is available only for non-key columns."

            if values["hds_config"] is not None:
                assert (
                    values["hds_config"].hds_table_type != HdsTableType.SCD2
                ), "Column casting is not currently supported for HDS SCD2 tables."
        return values

    @root_validator(pre=True)
    def valid_new_column_udfs(cls, values):
        if values["new_column_udfs"] is not None:
            assert all(
                [from_dict(data_class=ColumnUDFConfig, data=values["new_column_udfs"][col]) for col in values["new_column_udfs"].keys()]
            ), "New column UDFs must only contain 'function' and 'output_type' keys with corresponding values."

            if values["hds_config"] is not None:
                assert (
                    values["hds_config"].hds_table_type != HdsTableType.SCD2
                ), "New column UDFs is not currently supported for HDS SCD2 tables."
        return values
