
def get_dlp_results_per_column(table_name):
    """SELECT
        locations.record_location.field_id.name AS field_name,
        info_type.name,
    COUNT(*) AS count_total
    FROM
        `{table_name}}`,
        UNNEST(location.content_locations) AS locations
    GROUP BY
            locations.record_location.field_id.name as column_name,
            info_type.name as info_type_name,
            likelihood as likelihood
    HAVING
            count_total>0 """.format(table_name = table_name)