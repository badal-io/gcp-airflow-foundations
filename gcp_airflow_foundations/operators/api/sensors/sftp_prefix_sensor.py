#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from typing import Optional
import logging
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sensors.base import BaseSensorOperator
from gcp_airflow_foundations.common.sftp.helpers import save_id_to_file


class SFTPFilesExistencePrefixSensor(BaseSensorOperator):
    """
    Waits for multiple file or directories to be present on SFTP.
    :param path: lit of remote files or directory paths
    :type path: str
    :param sftp_conn_id: The connection to run the sensor against
    :type sftp_conn_id: str
    """

    template_fields = ('path', 'prefixes',)

    def __init__(self, *, path: str, prefixes: list, key_auth=False, key_name="", sftp_conn_id: str = 'sftp_default', **kwargs) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.prefixes = prefixes
        self.key_auth = key_auth
        self.key_name = key_name
        self.hook: Optional[SFTPHook] = None
        self.sftp_conn_id = sftp_conn_id

        if key_auth:
            save_id_to_file(self.key_name)

    def poke(self, context: dict) -> bool:
        self.hook = SFTPHook(self.sftp_conn_id)

        self.prefixes = [f.split("/")[-1] for f in self.prefixes]

        dir = self.hook.list_directory(self.path)
        logging.info(self.prefixes)
        for prefix in self.prefixes:
            logging.info(f'Poking for {prefix}')
            file_exists = False
            for file in dir:
                logging.info(file)
                if file.startswith(prefix):
                    file_exists = True
            if not file_exists:
                return False
        return True
