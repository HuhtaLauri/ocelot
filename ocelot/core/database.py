from typing import List, Dict, Any
from sqlalchemy import Column, Table
from psycopg2.extensions import cursor
from psycopg2.errors import DataError
from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base
from ocelot.core.diff import Diff
from ocelot.core.change import TableChange, ColumnChange, OperationType


class Database:
    def __init__(self):
        self.tables: List[Table] = []
        self.sync_databases = []
        self.base = declarative_base(metadata=MetaData())

    def compare_tables(self, db2: "Database") -> Diff:
        """
        Compare the existing of tables by schema and tables
        with self and passed Database

        For each change create a Change object and append it
        to diff.changes
        """
        diff = Diff()
        this_tables = set(self.base.metadata.tables)
        that_tables = set(db2.base.metadata.tables)

        common_tables = this_tables.intersection(that_tables)
        new_tables = this_tables.difference(that_tables)
        dropped_tables = that_tables.difference(this_tables)

        if new_tables:
            for table in new_tables:
                add_table = self.base.metadata.tables[table]
                change = TableChange(operation=OperationType.ADD, table=add_table)
                diff.add_change(change)

        if dropped_tables:
            for table in dropped_tables:
                drop_table = df_2.base.metadata.tables[table]
                change = TableChange(operation=OperationType.DROP, table=drop_table)
                diff.add_change(change)

        return diff

    def compare_columns(self, db2: "Database") -> Diff:
        diff = Diff()

        this_columns = set(self.collect_columns())
        that_columns = set(db2.collect_columns())

        print(this_columns)
        print(that_columns)

        common_columns = this_columns.intersection(that_columns)

        print(common_columns)

        return diff

    def collect_columns(self) -> List[Column]:
        columns = []
        for _, table in self.base.metadata.tables.items():
            for column in table.columns:
                columns.append(column)

        return columns

    @staticmethod
    def db_result_to_json(cursor: cursor) -> List[Dict[str, Any]]:

        # Get the column names from the cursor description
        if not cursor.description:
            raise DataError

        columns = [desc[0] for desc in cursor.description]

        # Fetch all rows from the cursor
        rows = cursor.fetchall()

        # Create a list of dictionaries, each representing a row
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))

        # Convert the list of dictionaries to a JSON string
        return results


if __name__ == "__main__":
    pass
