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

    params["extra_options"] = convert_template_extra_options(template_config, i)

    return SourceTableConfig(**params)


def convert_template_extra_options(template_config: SourceTemplateConfig, i: int):
    """
    Converts the extra_options field in a SourceTemplateConfig to match a corresponding SourceTableConfig.
    """
    template_extra_options = getattr(template_config, "extra_options")
    table_extra_options = {}
    for config_option_name, _ in template_extra_options.items():
        # get each second-level dict in extra_options corresponding to a particular source specific config
        config_options = template_extra_options[config_option_name]
        table_config_options = {}
        # loop through the config options in that config
        for key, val in config_options.items():
            if key in template_config.iterable_options:
                # if the config name matches to a value in iterable_options, parse as a list
                table_config_options[key] = val[i]
            else:
                # else, value applies to all tables
                table_config_options[key] = val
        table_extra_options[config_option_name] = table_config_options
    return table_extra_options
