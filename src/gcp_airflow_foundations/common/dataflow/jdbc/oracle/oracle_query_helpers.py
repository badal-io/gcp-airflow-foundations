import logging

def get_query_for_oracle_load_full(table_name, columns, owner):
    """
    JDBC query for full ingestion of one table
    """

    logging.info(f"BUILDING FULL QUERY for {table_name}")

    select_cols = ",".join(str(x) for x in columns)

    return f'select {select_cols} from {owner}.{table_name}'

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

    cols = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_ID"]
    cols = ",".join(cols)

    return f"""select {cols} from alL_tab_columns where OWNER = '{owner}'"""


def get_table_schema_query(schema_table, source_table_name):
    """
    Query to get source schema for table from BQ (previously ingested).
    """

    return f" select distinct * from (select COLUMN_NAME, DATA_TYPE from `{schema_table}` \
        where TABLE_NAME = '{source_table_name}' order by COLUMN_ID)"


def oracle_mappings():
    return {
        # STRING
        "VARCHAR2": "STRING",
        "NVARCHAR2": "STRING",
        "CHAR": "STRING",
        "NCHAR": "STRING",
        "CLOB": "STRING",
        "NCLOB": "STRING",
        "INTERVAL YEAR TO MONTH": "STRING",
        "INTERVAL DAY TO SECOND": "STRING",
        "BFILE": "STRING",
        "ROWID": "STRING",
        # INT64
        "INTEGER": "INT64",
        "SHORTINTEGER": "INT64",
        "LONGINTEGER": "INT64",
        "NUMBER(x, -y)": "INT64",
        "NUMBER(x)": "INT64",
        # NUMERIC
        "NUMBER": "NUMERIC",
        "FLOAT": "NUMERIC",
        "BINARY_DOUBLE": "NUMERIC",
        "BINARY_FLOAT": "NUMERIC",
        "NUMBER(*, x)": "NUMERIC",
        # BYTES
        "LONG": "BYTES",
        "BLOB": "BYTES",
        "RAW": "BYTES",
        "LONG RAW": "BYTES",
        # DATE
        "DATE": "DATE",
        # TIMESTAMP
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP()": "TIMESTAMP",
        "TIMESTAMP WITH TIMEZONE": "TIMESTAMP",
        "TIMESTAMP WITH LOCAL TIME ZONE": "TIMESTAMP"
    }

def oracle_to_bq(
    dtypes: list
):
    mappings = oracle_mappings()
    # Dicts are not really intended for use in this way, but for a tiny one
    # this is just easier
    for i in range(len(dtypes)):
        dtypes[i] = mappings[dtypes[i]]
    return dtypes