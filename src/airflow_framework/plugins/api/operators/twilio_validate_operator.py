from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from twilio.rest import Client
from typing import Any
import logging


class ValidateNumberOperator(BaseOperator):
    """
        Gets a column of phone numbers from a Bigquery table,
        validates these phone numbers using the Twilio Validate API,
        and merges result of the validated phone number back into the source table as a column 'is_valid' (boolean).

        connections require: twilio & GCP

        Attributes:
        bigquery_conn_id
        twilio_conn_id
        project: google project ID
        dataset: dataset ID
        temp_table: temporary table name for storing validation data (phone_number: string, is_valid: boolean)
        source_table: contain column with phone numbers for validation
        phone_number_column_name: column name of source table containing phone numbers for validation (string)
        """

    @apply_defaults
    def __init__(self, *, bigquery_conn_id: str, twilio_conn_id: str, project: str, dataset: str, temp_table: str, source_table: str, phone_number_column_name: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.bq_hook = self.get_bq_hook(bigquery_conn_id)
        self.cursor = self.bq_hook.get_conn().cursor()

        self.twilio_client = self.get_twilio_client(twilio_conn_id)

        self.project = project
        self.dataset = dataset
        self.source_table = source_table
        self.phone_number_column_name = phone_number_column_name
        self.temp_table = 'temp_table_for_twilio_phone_number_validation'



    def execute(self, context):

        # 1. create temp table in BigQuery
        self.create_temp_table()

        # 2. update BigQuery with valid phone numbers from Twilio
        self.update_validation()

        # 3. add column 'is_valid' to source table in BigQuery
        self.add_column()

    def get_bq_hook(self, bq_conn_id):
        try:
            hook = BigQueryHook(bq_conn_id, use_legacy_sql=False)
            logging.info("BigQuery connection established")
            return hook
        except Exception as e:
            raise AirflowException(f"Error BigQuery connection not established: {e}")

    def get_twilio_client(self, conn_id):
        try:
            connection = BaseHook.get_connection(conn_id)
            account_sid = connection.login
            auth_token = connection.password
            twilioClient = Client(account_sid, auth_token)
            logging.info("Twilio Connection established")
            return twilioClient
        except Exception as e:
            raise AirflowException(f"Error Twilio connection not established: {e}")

    def create_temp_table(self):
        self.bq_hook.create_empty_table(
            project_id=self.project,
            dataset_id=self.dataset,
            table_id=self.temp_table,
            # exist_ok=True,
            schema_fields=[
                {'name':'phone_number', 'type':'STRING'},
                {'name':'is_valid', 'type':'BOOL'}
            ]
        )

    def update_validation(self):
        # get list of phone numbers from source table in BigQuery
        self.cursor.execute(
            f'''SELECT {self.phone_number_column_name} FROM {self.project}.{self.dataset}.{self.source_table}''')
        result = self.cursor.fetchall()
        fetched_numbers = []
        for x in result:
            fetched_numbers.append(x[0])

        # validate list of phone numbers with Twilio API
        valid_number_list = []
        invalid_number_list =[]

        for number in fetched_numbers:
            is_valid = self.process_number(number)
            if is_valid:
                valid_number_list.append(number)
            if not is_valid:
                invalid_number_list.append(number)

        # write validated number to temp table in BigQuery
        sql_str = ''
        for x in valid_number_list:
            sql_str += f'("{x}", True),\n'
        for x in invalid_number_list:
            sql_str += f'("{x}", False),\n'
        sql_str = 'VALUES \n' + sql_str[0:-2]

        self.bq_hook.run_query(sql=f'''INSERT INTO {self.project}.{self.dataset}.{self.temp_table}
                                    (phone_number, is_valid)
                                    {sql_str}''',
                               write_disposition='WRITE_TRUNCATE',
                               use_legacy_sql=False)

    def process_number(self, number):
        try:
            self.twilio_client.lookups.v1.phone_numbers(number).fetch(country_code='US')
            return True
        except:
            return False

    def add_column(self):
        self.cursor.execute(f'''MERGE `{self.project}.{self.dataset}.{self.source_table}` AS TARGET
                            USING `{self.project}.{self.dataset}.{self.temp_table}` AS SOURCE
                            ON (TARGET.{self.phone_number_column_name} = SOURCE.phone_number)
                            WHEN MATCHED THEN UPDATE
                            SET TARGET.is_valid = SOURCE.is_valid''')