import os
import tempfile
import logging
from typing import Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
from airflow.providers.google.cloud.transfers.salesforce_to_gcs import SalesforceToGcsOperator

from airflow import AirflowException


class SalesforceToGcsQueryOperator(SalesforceToGcsOperator):
    """
    Generates a Salesforce query, submits it, and uploads results to Google Cloud Storage
    .. seealso::
        Extends the SalesforceToGcsOperator, see the link
        :ref:`howto/operator:SalesforceToGcsOperator`
    :param salesforce_object: The object to query in Salesforce
    :type salesforce_object: str
    :param ingest_all_fields: Whether or not to Select * the Object in Salesforce (if True, 
        then parameters fields_to_omit and fields_to_ingest are ignored)
    :type ingest_all_fields:  bool
    :param fields_to_omit: A list of fields to omit from the query (if included, 
        then parameters ingest_all_fields and fields_to_omit are ignored)
    :type fields_to_omit: List[str]
    :param fields_to_ingest: A list of fields to include in the query (if included, 
        then parameters ingest_all_fields and fields_to_omit are ignored)
    :type fields_to_ingest: List[str]
    :param bucket_name: The bucket to upload to.
    :type bucket_name: str
    :param object_name: The object name to set when uploading the file.
    :type object_name: str
    :param salesforce_conn_id: the name of the connection that has the parameters
        we need to connect to Salesforce.
    :type salesforce_conn_id: str
    :param include_deleted: True if the query should include deleted records.
    :type include_deleted: bool
    :param query_params: Additional optional arguments
    :type query_params: dict
    :param export_format: Desired format of files to be exported.
    :type export_format: str
    :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
        False if you want them to be left in the same format as they were in Salesforce.
        Leaving the value as False will result in datetimes being strings. Default: False
    :type coerce_to_timestamp: bool
    :param record_time_added: True if you want to add a Unix timestamp field
        to the resulting data that marks when the data was fetched from Salesforce. Default: False
    :type record_time_added: bool
    :param gzip: Option to compress local file or file data for upload
    :type gzip: bool
    :param gcp_conn_id: the name of the connection that has the parameters we need to connect to GCS.
    :type gcp_conn_id: str
    """

    template_fields = (
        'ingest_all_fields',
        'fields_to_omit',
        'fields_to_include'
    )
    template_ext = ('.sql',)

    def __init__(
        self,
        *,
        salesforce_object: str,
        ingest_all_fields: bool,
        fields_to_omit,
        fields_to_include,
        **kwargs,
    ):
        super().__init__(query = "", **kwargs)
        self.salesforce_object = salesforce_object
        self.ingest_all_fields = ingest_all_fields
        self.fields_to_omit = fields_to_omit
        self.fields_to_include = fields_to_include

    def execute(self, context: Dict):
        fields_to_query = []

        sf_hook = SalesforceHook(salesforce_conn_id=self.salesforce_conn_id)
        fields = sf_hook.get_available_fields(obj=self.salesforce_object)
        logging.info(fields)

        if self.ingest_all_fields:
            # Query all fields
            fields_to_query = fields
        elif self.fields_to_omit:
            # Query all fields except [x_1,....x_n]
            fields_to_query = list(set(fields) - set(self.fields_to_omit))
        elif self.fields_to_include:
            # Query specified fields
            fields_to_query = self.fields_to_include
            if not set(fields_to_query).issubset(set(fields)):
                extra_fields = list(set(fields_to_query) - set(fields))
                raise AirflowException(f"Queryable content for Salesforce object {self.salesforce_object} included fields not present in the object's schema: {extra_fields}")
        else:
            # ingest_all_fields is false, and no other information was provided - throw exception
            raise AirflowException(f"Queryable content for Salesforce object {self.salesforce_object} was malformed")
        query = f"""SELECT {",".join(fields_to_query)} FROM {self.salesforce_object}""".strip()
        logging.info(query)
        self.query = query

        super().execute(context)
