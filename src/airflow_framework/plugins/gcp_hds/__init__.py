from airflow.plugins_manager import AirflowPlugin
from airflow_framework.plugins.gcp_hds.hds_merge_table_operator import MergeBigQueryHDS

class AirflowBadalPluginHDS(AirflowPlugin):
    name = "badal_plugin_hds"
    operators = [MergeBigQueryHDS]
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
