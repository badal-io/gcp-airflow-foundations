
def get_dlp_results_sql(project_id, dataset_id, table_id, min_count=0):
    """ Generate sql to query the DLP results table: https://cloud.google.com/dlp/docs/querying-findings
    :param project_id:
    :param dataset_id:
    :param table_id:
    :return:
    """

    return """SELECT 
        locations.record_location.field_id.name AS field_name, 
        info_type.name as info_type_name,
        likelihood as likelihood,
        COUNT(*) AS count_total
    FROM {}.{}.{},
        UNNEST(location.content_locations) AS locations
    GROUP BY
        locations.record_location.field_id.name,
        info_type.name
        likelihood
    HAVING  count_total>{}}
    """.format(project_id, dataset_id, table_id, min_count)

def fields_to_policy_tags(fields, tag):
    tags = []
    for field in fields:
        tags.append({"name": field, "policyTags": {'names': [tag]}})
    return tags
