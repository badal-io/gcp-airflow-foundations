from dacite import Config
from dacite.exceptions import MissingValueError
from dacite.dataclasses import DefaultValueNotFoundError

from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig
from enum import Enum
import yaml
import dacite
import logging
import glob

from airflow.exceptions import AirflowException

def load_tables_config(config_path) -> DataSourceTablesConfig:
    table_config_file = open(config_path, "r").read()
    raw_config = yaml.load(table_config_file, Loader=yaml.FullLoader)

    try:
        config = dacite.from_dict(
            data_class=DataSourceTablesConfig, data=raw_config, config=Config(cast=[Enum])
        )
        logging.info(f"Config is {config}")
    except (MissingValueError, DefaultValueNotFoundError, KeyError) as e:
        raise AirflowException(str(e))

    return config


def load_tables_config_from_dir(dir_path):
    files = glob.glob(f"{dir_path}/*.yaml")
    logging.info(f"Loading config from  {files}")
    return list(map(load_tables_config, files))
