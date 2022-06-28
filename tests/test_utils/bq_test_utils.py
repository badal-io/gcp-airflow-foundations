import pandas
from time import sleep
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def insert_to_bq_from_csv(csv, project_id, dataset_id, table_id):
    gcp_conn_id = "google_cloud_default"
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    # client = bigquery.Client(project=project_id)
    client = bq_hook.get_client(project_id=project_id)
    table = f"{project_id}.{dataset_id}.{table_id}"
    df = pandas.read_csv(csv)
    load_job = client.load_table_from_dataframe(df, table)
    while load_job.running():
        sleep(1)


def insert_to_bq_from_dict(data, project_id, dataset_id, table_id):
    gcp_conn_id = "google_cloud_default"
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = bq_hook.get_client(project_id=project_id)
    table = f"{project_id}.{dataset_id}.{table_id}"
    df = pandas.DataFrame.from_dict(data)
    load_job = client.load_table_from_dataframe(df, table)
    while load_job.running():
        sleep(1)
