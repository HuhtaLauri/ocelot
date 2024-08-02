from ocelot.core.utils import cursor
from dotenv import load_dotenv
import os
from ocelot.core.utils import db_result_to_json
from ocelot.core.database import Database
from tqdm import tqdm
from sqlalchemy import Table, Column, String
from sqlalchemy.orm import DeclarativeBase
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
    "VARCHAR": VARCHAR
}

load_dotenv()

class Base(DeclarativeBase):
    pass

class PostgresDatabase(Database):

    def __init__(self):
        super().__init__()
        self.connections_string = os.environ.get(
            "CONNECTION_STRING",
            "postgresql://postgres:postgres@localhost:5432/postgres"
        )
        self.tables_metadata = self.get_tables_metadata()
        self.columns_metadata = self.get_columns_metadata()

        self.populate_table_data()

    def get_tables_metadata(self):
        query = f"""
            SELECT *
            FROM information_schema.tables
        """ 

        with cursor(self.connections_string) as cur:
            cur.execute(query)
            result_json = db_result_to_json(cur)

        return result_json

    def get_columns_metadata(self):
        query = f"""
            SELECT *
            FROM information_schema.columns
        """

        with cursor(self.connections_string) as cur:
            cur.execute(query)
            result_json = db_result_to_json(cur)

        return result_json


    def populate_table_data(self):
        """
        Creates table objects from metadata
        and appends them to self.tables list
        """
        for table in tqdm(self.tables_metadata):
            table_name = table["table_name"]
            schema_name = table["table_schema"]

            table_obj = Table(table_name, Base.metadata)

            # Get only relevant column data
            table_column_json = []
            for metadata in self.columns_metadata:
                if metadata["table_name"] == table_name and metadata["table_schema"] == schema_name:
                    table_column_json.append(metadata)

            # Add columns
            for column in table_column_json:
                column_type = postgres_types_to_sqlalchemy.get(str(column["data_type"]).upper(), String)
                print(column_type)
                if column_type in [ARRAY]:
                    table_obj.append_column(Column(column["column_name"], ARRAY(String)))
                else:
                    table_obj.append_column(Column(column["column_name"], column_type))

            self.tables.append(table_obj)

