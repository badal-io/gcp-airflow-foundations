import logging

def get_query_for_mysql_load_full(table_name, columns, owner):
    """
    JDBC query for full ingestion of one table
    """

    logging.info(f"BUILDING FULL QUERY for {table_name}")

    select_cols = ",".join(str(x) for x in columns)

    return f'select {select_cols} from {table_name}'

def convert_schema_to_json(lists, labels):
    """
    Input: 
        lists: non-empty list of n lists each of length x
        labels: list of strings of length n
    Output:
        list of x dictionaries with n entries, each row corresponding
        to a labelled row (merged from lists)
    """
    dicts = []
    for i in range(len(lists[0])):
        dict = {}
        for j in range(len(labels)):
            dict[labels[j]] = lists[j][i]
        dicts.append(dict)
    return dicts     

def cast_columns(columns, dtypes, casts):
    # perform the Oracle castings needed within the query itself.
    castings = []
    for c,d in zip(columns, dtypes):
        if d in casts:
            cast = casts[d].replace("COLUMN", c)
            castings.append(cast)
        else:
            castings.append(c)
    return castings


def get_schema_query(owner):
    """
    Oracle query for getting schema information for all tables
    """

    cols = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "ORDINAL_POSITION"]
    cols = ",".join(cols)

    return f"""select {cols} from INFORMATION_SCHEMA.COLUMNS"""


def get_table_schema_query(schema_table, source_table_name):
    """
    Query to get source schema for table from BQ (previously ingested).
    """

    return f" select distinct * from (select COLUMN_NAME, DATA_TYPE from `{schema_table}` \
        where TABLE_NAME = '{source_table_name}' order by ORDINAL_POSITION)"


def mysql_mappings():
    return {
        "TINYINT": "INT64",
        "SMALLINT": "INT64",
        "MEDIUMINT": "INT64",
        "INT": "INT64",
        "BIGINT": "INT64",
        "DECIMAL": "NUMERIC",
        "FLOAT": "FLOAT64",
        "DOUBLE": "FLOAT64",
        "BIT": "BOOL",
        "CHAR": "STRING",
        "VARCHAR": "STRING",
        "TINYTEXT": "STRING",
        "TEXT": "STRING",
        "MEDIUMTEXT": "STRING",
        "LONGTEXT": "STRING",
        "BINARY": "BYTES",
        "VARBINARY": "BYTES",
        "DATE": "DATE",
        "TIME": "TIME",
        "DATETIME": "DATETIME",
        "TIMESTAMP": "TIMESTAMP",
    }

def mysql_to_bq(
    dtypes: list
):
    mappings = mysql_mappings()
    # Dicts are not really intended for use in this way, but for a tiny one
    # this is just easier
    for i in range(len(dtypes)):
        dtypes[i] = mappings[dtypes[i]]
    return dtypes