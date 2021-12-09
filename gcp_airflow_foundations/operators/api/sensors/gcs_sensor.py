import os
from re import L
import textwrap
import warnings
from datetime import datetime
from typing import Callable, List, Optional, Sequence, Set, Union
import logging
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sensors.base import BaseSensorOperator, poke_mode_only


class GCSObjectListExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of a list of files in Google Cloud Storage.
    :param bucket: The Google Cloud Storage bucket where the object is.
    :type bucket: str
    :param objects: A list of the objects to check in the Google cloud
        storage bucket.
    :type object: list[str]
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'bucket',
        'objects',
        'impersonation_chain',
    )
    ui_color = '#f0eee4'

    def __init__(
        self,
        *,
        bucket: str,
        objects: list,
        google_cloud_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.bucket = bucket
        self.objects = objects
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: dict) -> bool:
        for object in self.objects:
            self.log.info('Sensor checks existence of : %s, %s', self.bucket, object)
        hook = GCSHook(
            gcp_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        all_files_exist = True
        for object in self.objects:
            logging.info(object)
            if not hook.exists(self.bucket, object):
                all_files_exist = False

        return all_files_exist