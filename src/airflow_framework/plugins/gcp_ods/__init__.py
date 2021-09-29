from airflow.plugins_manager import AirflowPlugin
from airflow_framework.plugins.gcp_ods.ods_merge_table_operator import MergeBigQueryODS

class AirflowBadalPluginODS(AirflowPlugin):
    name = "badal_plugin_ods"
    operators = [MergeBigQueryODS]
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
