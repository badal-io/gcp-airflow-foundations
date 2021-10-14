from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.bigquery import (
    BigQueryBaseCursor,
    BigQueryConnection,
    BigQueryCursor,
    BigQueryHook
)
from typing import Any, Callable, Dict, Optional, Type
from twilio.rest import Client



class ValidateNumberOperator(BaseOperator):
    """
        Gets a column of phone numbers from a Bigquery table,
        validates these phone numbers using the Twilio Validate API,
        and merges result of phone number validation back into the source table as a column 'is_valid' (boolean).

        connections requirement: twilio & google

        Attributes:
        bigquery_conn_id
        twilio_conn_id
        project: google project ID
        dataset: dataset ID
        temp_table: temporary table name for storing validation data (phone_number: string, is_valid: boolean)
        source_table: contain column with phone numbers for validation
        phone_number_column_name: column name of source table containing phone numbers for validation (string)
        """

    twilioClient = None

    @apply_defaults
    def __init__(self, *, bigquery_conn_id: str, twilio_conn_id: str, project: str, dataset: str, temp_table: str, source_table: str, phone_number_column_name: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.bigquery_conn_id = bigquery_conn_id
        self.twilio_conn_id = twilio_conn_id
        self.project = project
        self.dataset = dataset
        self.temp_table = temp_table
        self.source_table = source_table
        self.phone_number_column_name = phone_number_column_name

        # check if twilioClient is available before connecting
        if ValidateNumberOperator.twilioClient is None:
            try:
                connection = BaseHook.get_connection(self.twilio_conn_id)
                account_sid = connection.login
                auth_token = connection.password
                ValidateNumberOperator.twilioClient = Client(account_sid, auth_token)
            except Exception as error:
                print("Error: Connection not established {}".format(error))
            else:
                print("Connection established")
        self.twilio_client = ValidateNumberOperator.twilioClient

    def execute(self, context):
        # 1. get list of phone numbers from BigQuery column
        number_list = self.get_from_bq()

        # 2. validate phone numbers with Twilio Validate API
        valid_number_list = []
        invalid_number_list =[]

        for number in number_list:
            is_valid = self.process_number(number)
            if is_valid:
                valid_number_list.append(number)
            if not is_valid:
                invalid_number_list.append(number)
        print("number of invalid numbers ------------>", len(invalid_number_list))
        print("number of invalid numbers ------------>", len(valid_number_list))

        # 3. update temp table in bq
        self.update_bq(valid_number_list, invalid_number_list)

        # 4. add is_valid column to source table
        self.add_column()

        # 5. truncate temporary table
        self.truncate_temp_table() # works but no point if you have to add the temp table automatically

    def get_from_bq(self):
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'''SELECT {self.phone_number_column_name} FROM {self.project}.{self.dataset}.{self.source_table}''')
        result = cursor.fetchall()

        fetched_numbers = []
        for x in result:
            fetched_numbers.append(x[0])

        return fetched_numbers

    def process_number(self, number):
        try:
            phone_number = self.twilio_client.lookups.v1.phone_numbers(number).fetch(country_code='US')
            print(phone_number.phone_number)
            return True
        except:
            return False

    def update_bq(self, valid_numbers, invalid_numbers):

        sql_str = ''
        for x in valid_numbers:
            sql_str += f'("{x}", True),\n'
        for x in invalid_numbers:
            sql_str += f'("{x}", False),\n'
        sql_str = 'VALUES \n' + sql_str[0:-2]

        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f'''INSERT INTO {self.project}.{self.dataset}.{self.temp_table} 
                            (phone_number, is_valid)
                            {sql_str}''')

    def add_column(self):
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f'''MERGE `{self.project}.{self.dataset}.{self.source_table}` AS TARGET
                            USING `{self.project}.{self.dataset}.{self.temp_table}` AS SOURCE
                            ON (TARGET.phone_number = SOURCE.{self.phone_number_column_name})
                            WHEN MATCHED THEN UPDATE
                            SET TARGET.is_valid = SOURCE.is_valid''')

    def truncate_temp_table(self):
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f'''TRUNCATE TABLE `{self.project}.{self.dataset}.{self.temp_table}`''')