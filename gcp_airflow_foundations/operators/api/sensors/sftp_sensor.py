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

from paramiko.sftp import SFTP_NO_SUCH_FILE

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sensors.base import BaseSensorOperator


class SFTPFilesExistenceSensor(BaseSensorOperator):
    """
    Waits for multiple file or directories to be present on SFTP.
    :param path: lit of remote files or directory paths
    :type path: str
    :param sftp_conn_id: The connection to run the sensor against
    :type sftp_conn_id: str
    """

    template_fields = ('paths',)

    def __init__(self, *, paths: list, sftp_conn_id: str = 'sftp_default', **kwargs) -> None:
        super().__init__(**kwargs)
        self.paths = paths
        self.hook: Optional[SFTPHook] = None
        self.sftp_conn_id = sftp_conn_id

    def poke(self, context: dict) -> bool:
        self.hook = SFTPHook(self.sftp_conn_id)
        for path in self.paths:
            self.log.info('Poking for %s', path)
            try:
                mod_time = self.hook.get_mod_time(path)
                self.log.info('Found File %s last modified: %s', str(path), str(mod_time))
            except OSError as e:
                if e.errno != SFTP_NO_SUCH_FILE:
                    raise e
                return False
            self.hook.close_conn()
        return True