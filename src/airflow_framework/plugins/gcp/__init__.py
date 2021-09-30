from airflow.plugins_manager import AirflowPlugin
from airflow_framework.plugins.gcp.ods.ods_merge_table_operator import MergeBigQueryODS
from airflow_framework.plugins.gcp.hds.hds_merge_table_operator import MergeBigQueryHDS

class AirflowBadalPluginBigQuery(AirflowPlugin):
    name = "badal_plugin_bigQuery"
    operators = [MergeBigQueryODS, MergeBigQueryHDS]
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
