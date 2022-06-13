from gcp_airflow_foundations.base_class.source_table_config import SourceTableConfig
from gcp_airflow_foundations.base_class.source_template_config import SourceTemplateConfig
import logging


def convert_template_to_table(template_config: SourceTemplateConfig, table_name: str, i: int):

    landing_zone_table_name_override = \
        template_config.landing_zone_table_name_override_template.replace("{table}", table_name)
    dest_table_override = \
        template_config.dest_table_override_template.replace("{table}", table_name)

    if template_config.surrogate_keys:
        surrogate_keys = template_config.surrogate_keys["table"]
    else:
        surrogate_keys = []

    params = {
        "table_name": table_name,
        "ingestion_type": template_config.ingestion_type,
        "landing_zone_table_name_override": landing_zone_table_name_override,
        "dest_table_override": dest_table_override,
        "surrogate_keys": surrogate_keys,
        "start_date_tz": template_config.start_date_tz,
        "version": template_config.version,
        "catchup": template_config.catchup,
    }

    optional_fields = [
        "dest_table_override",
        "surrogate_keys",
        "column_mapping",
        "cluster_fields",
        "column_casting",
        "new_column_udfs",
        "ods_config",
        "hds_config",
        "start_date",
    ]

    for of in optional_fields:
        if hasattr(template_config, of):
            params[of] = getattr(template_config, of)

    template_extra_options = getattr(template_config, "extra_options")
    table_extra_options = {}
    for config_option_name, _ in template_extra_options.items():
        config_options = template_extra_options[config_option_name]
        table_config_options = {}
        for key, val in config_options.items():
            if key in template_config.iterable_options:
                table_config_options[key] = val[i]
            else:
                table_config_options[key] = val
        table_extra_options[config_option_name] = table_config_options
    params["extra_options"] = table_extra_options

    return SourceTableConfig(**params)
