from ocelot.core.utils import cursor
from dotenv import load_dotenv
from ocelot.core.database import Database
from sqlalchemy import Table, Column, String
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    BIGINT,
    BIT,
    BOOLEAN,
    BYTEA,
    CHAR,
    CIDR,
    CITEXT,
    DATE,
    DATEMULTIRANGE,
    DATERANGE,
    DOMAIN,
    DOUBLE_PRECISION,
    ENUM,
    FLOAT,
    HSTORE,
    INET,
    INT4MULTIRANGE,
    INT4RANGE,
    INT8MULTIRANGE,
    INT8RANGE,
    INTEGER,
    INTERVAL,
    JSON,
    JSONB,
    JSONPATH,
    MACADDR,
    MACADDR8,
    MONEY,
    NUMERIC,
    NUMMULTIRANGE,
    NUMRANGE,
    OID,
    REAL,
    REGCLASS,
    REGCONFIG,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    TSMULTIRANGE,
    TSQUERY,
    TSRANGE,
    TSTZMULTIRANGE,
    TSTZRANGE,
    TSVECTOR,
    UUID,
    VARCHAR,
)

postgres_types_to_sqlalchemy = {
    "ARRAY": ARRAY,
    "BIGINT": BIGINT,
    "BIT": BIT,
    "BOOLEAN": BOOLEAN,
    "BYTEA": BYTEA,
    "CHAR": CHAR,
    "CIDR": CIDR,
    "CITEXT": CITEXT,
    "DATE": DATE,
    "DATEMULTIRANGE": DATEMULTIRANGE,
    "DATERANGE": DATERANGE,
    "DOMAIN": DOMAIN,
    "DOUBLE_PRECISION": DOUBLE_PRECISION,
    "ENUM": ENUM,
    "FLOAT": FLOAT,
    "HSTORE": HSTORE,
    "INET": INET,
    "INT4MULTIRANGE": INT4MULTIRANGE,
    "INT4RANGE": INT4RANGE,
    "INT8MULTIRANGE": INT8MULTIRANGE,
    "INT8RANGE": INT8RANGE,
    "INTEGER": INTEGER,
    "INTERVAL": INTERVAL,
    "JSON": JSON,
    "JSONB": JSONB,
    "JSONPATH": JSONPATH,
    "MACADDR": MACADDR,
    "MACADDR8": MACADDR8,
    "MONEY": MONEY,
    "NUMERIC": NUMERIC,
    "NUMMULTIRANGE": NUMMULTIRANGE,
    "NUMRANGE": NUMRANGE,
    "OID": OID,
    "REAL": REAL,
    "REGCLASS": REGCLASS,
    "REGCONFIG": REGCONFIG,
    "SMALLINT": SMALLINT,
    "TEXT": TEXT,
    "TIME": TIME,
    "TIMESTAMP": TIMESTAMP,
    "TSMULTIRANGE": TSMULTIRANGE,
    "TSQUERY": TSQUERY,
    "TSRANGE": TSRANGE,
    "TSTZMULTIRANGE": TSTZMULTIRANGE,
    "TSTZRANGE": TSTZRANGE,
    "TSVECTOR": TSVECTOR,
    "UUID": UUID,
    "VARCHAR": VARCHAR,
}

load_dotenv()


class PostgresDatabase(Database):

    def __init__(self, connection_string):
        super().__init__()
        self.connections_string = connection_string
        self.tables_metadata = self.get_tables_metadata()
        self.columns_metadata = self.get_columns_metadata()

        self.populate_table_data()

    def get_tables_metadata(self):
        query = f"""
            SELECT *
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """

        with cursor(self.connections_string) as cur:
            cur.execute(query)
            result_json = self.db_result_to_json(cur)

        return result_json

    def get_columns_metadata(self):
        query = f"""
            WITH
                cte AS (
                    SELECT c.table_schema,
                           c.table_name,
                           c.column_name,
                           c.data_type,
                           c.is_nullable,
                           c.character_maximum_length,
                           ARRAY_AGG(tc.constraint_type) AS constraints
                    FROM information_schema.columns c
                    LEFT JOIN 
                        information_schema.key_column_usage kcu 
                        ON c.table_schema = kcu.table_schema
                        AND c.table_name = kcu.table_name 
                        AND c.column_name = kcu.column_name
                    LEFT JOIN 
                        information_schema.table_constraints tc 
                        ON kcu.constraint_name = tc.constraint_name 
                        AND kcu.table_schema = tc.table_schema
                        AND kcu.table_name = tc.table_name
                    WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')
                    GROUP BY c.table_schema,
                             c.table_name,
                             c.column_name,
                             c.data_type,
                             c.is_nullable,
                             c.character_maximum_length
                    )
                SELECT *,
                    CASE WHEN 'PRIMARY KEY' = ANY(constraints) THEN TRUE ELSE FALSE END AS primary_key,
                    CASE WHEN 'UNIQUE' = ANY(constraints) THEN TRUE ELSE FALSE END AS unique_key
                FROM cte
                ORDER BY table_schema, table_name;
        """

        with cursor(self.connections_string) as cur:
            cur.execute(query)
            result_json = self.db_result_to_json(cur)

        return result_json

    def populate_table_data(self):
        """
        Creates table objects from metadata
        and appends them to self.tables list
        """
        for table in self.tables_metadata:
            table_name = table["table_name"]
            schema_name = table["table_schema"]

            table_obj = Table(table_name, self.base.metadata, schema=schema_name)
            # Get only relevant column data
            table_column_json = []
            for metadata in self.columns_metadata:
                if (
                    metadata["table_name"] == table_name
                    and metadata["table_schema"] == schema_name
                ):
                    table_column_json.append(metadata)

            # Add columns
            for column in table_column_json:
                column_type = postgres_types_to_sqlalchemy.get(
                    str(column["data_type"]).upper(), String
                )
                if column_type in [ARRAY]:
                    table_obj.append_column(
                        Column(
                            column["column_name"],
                            ARRAY(String),
                            primary_key=column["primary_key"],
                            unique=column["unique_key"],
                        )
                    )
                else:
                    if column["character_maximum_length"]:
                        column_type = column_type(column["character_maximum_length"])

                    table_obj.append_column(
                        Column(
                            column["column_name"],
                            column_type,
                            primary_key=column["primary_key"],
                            unique=column["unique_key"],
                        )
                    )

            self.tables.append(table_obj)
