import json
import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook


from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from gcp_airflow_foundations.operators.api.hooks.twilio_hook import TwilioHook


class TwilioToBigQueryOperator(BaseOperator):
    """
    Twilio Text Message data to BigQuery
    
    Attributes:
        twilio_account_sid: Twilio Account SID to be used in the API
        project_id: GCP project ID               
        table_id: Staging table name    
        dataset_id: Staging dataset name
        schema_fields: Schema fields provided by the Airflow Framework for the Twilio Staging table
        delegate_to: The account to impersonate using domain-wide delegation of authority, if any
        gcp_conn_id: Airflow GCP connection ID
        twilio_conn_id: Airflow Twilio connection ID
        column_mapping: Column mapping dictionary
    """
    template_fields = ('dataset_id', 'table_id', 'project_id', 'labels')

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(self,
                 twilio_account_sid,
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

        self.twilio_account_sid = twilio_account_sid
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
        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        if not bq_hook.table_exists(project_id=self.project_id,
                                dataset_id=self.dataset_id,
                                table_id=self.table_id
                                ):

            if not self.schema_fields and self.gcs_schema_object:

                parsed_url = urlparse(self.gcs_schema_object)
                gcs_bucket = parsed_url.netloc
                gcs_object = parsed_url.path.lstrip('/')

                gcs_hook = GoogleCloudStorageHook(
                    google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                    delegate_to=self.delegate_to)
                schema_fields = json.loads(gcs_hook.download(
                    gcs_bucket,
                    gcs_object).decode("utf-8"))
            else:
                schema_fields = self.schema_fields
            
            cursor.create_empty_table(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                schema_fields=schema_fields,
                time_partitioning=self.time_partitioning,
                labels=self.labels,
                encryption_configuration=self.encryption_configuration
            )

        service = bq_hook.get_service()

        body = {
            'rows' : twilio_hook.get_text_messages(account_sid=self.twilio_account_sid)
        }

        service_exec = service.tabledata().insertAll(
            body = body,
            projectId = self.project_id,
            tableId = self.table_id,
            datasetId = self.dataset_id
        )

        results = service_exec.execute()

        logging.info(results)