from airflow.plugins_manager import AirflowPlugin
from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import BigQueryMergeTableOperator
from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator

class AirflowBadalPlugin(AirflowPlugin):
    name = "badal_plugin"
    operators = [BigQueryMergeTableOperator, BigQueryCreateTableOperator]
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
