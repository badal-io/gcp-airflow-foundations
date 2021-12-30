import os
import pytest
import json
from google.cloud.bigquery import SchemaField


@pytest.fixture(scope="session")
def mock_dlp_data_1():
    rows = [
        {
            "customerID": "customer_1",
            "email": "c1@gmail.com",
            "city_name": "Toronto"
        },
        {
            "customerID": "customer_2",
            "email": "c2@gmail.com",
            "city_name": "Montreal"
        },
        {
            "customerID": "customer_3",
            "email": "c3@hotmail.com",
            "city_name": "Ottawa"
        },
    ]

    return rows

@pytest.fixture(scope="session")
def mock_dlp_data_bq_schema_1():
    schema_fields = [
        {"name":"customerID", "type":"STRING", "mode":"NULLABLE"},
        {"name":"email", "type":"STRING", "mode":"NULLABLE"},
        {"name":"city_name", "type":"STRING", "mode":"NULLABLE"}
    ]
    return [SchemaField.from_api_repr(i) for i in schema_fields]
