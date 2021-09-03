import json
import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook


from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_framework.plugins.api.hooks.twilio_hook import TwilioHook


class TwilioToBigQueryOperator(BaseOperator):
    """
    Twilio Text Message data to BigQuery
    """
    template_fields = ('dataset_id', 'table_id', 'project_id', 'labels')

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(self,
                 dataset_id,
                 table_id,
                 project_id=None,
                 schema_fields=None,
                 gcs_schema_object=None,
                 time_partitioning=None,
                 gcp_conn_id='google_cloud_default',
                 twilio_conn_id='twilio_default',
                 delegate_to=None,
                 labels=None,
                 encryption_configuration=None,
                 *args, **kwargs):

        super(TwilioToBigQueryOperator, self).__init__(*args, **kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.gcs_schema_object = gcs_schema_object
        self.gcp_conn_id = gcp_conn_id
        self.twilio_conn_id = twilio_conn_id
        self.delegate_to = delegate_to
        self.time_partitioning = {} \
            if time_partitioning is None else time_partitioning
        self.labels = labels
        self.encryption_configuration = encryption_configuration


    def execute(self, context):
        twilio_hook = TwilioHook(twilio_conn_id=self.twilio_conn_id)
        
        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)

        service = bq_hook.get_service()

        body = {
            'rows' : twilio_hook.get_text_messages(account_sid='AC6e889487026563887777c14fdd2c4276')
        }

        service_exec = service.tabledata().insertAll(
            body = body,
            projectId = self.project_id,
            tableId = self.table_id,
            datasetId = self.dataset_id
        )

        results = service_exec.execute()

        logging.info(results)