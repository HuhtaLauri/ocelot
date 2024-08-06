from pydantic import BaseModel, model_validator, field_validator, ValidationError
from enum import Enum
from uuid import UUID, uuid4
import sqlalchemy
from sqlalchemy.sql.schema import Table, Column
from typing import Type
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.sql import ddl


class ChangeStatus(Enum):
    PENDING = "pending"
    APPLIED = "applied"
    ROLLED_BACK = "rolled_back"

    def __str__(self):
        return self.name


class OperationType(Enum):
    ADD = "add"
    DROP = "drop"
    ALTER = "alter"


class Change(BaseModel):
    id: UUID = uuid4()
    operation: OperationType
    status: ChangeStatus = ChangeStatus.PENDING

    class Config:
        arbitrary_types_allowed = True

    def construct_table_change_query(self):
        if not isinstance(self, TableChange):
            raise TypeError("Invalid Type. Must be an instance of TableChange class")

        operation_mapping = {
            OperationType.ADD: self.construct_add_table_query,
            OperationType.DROP: self.construct_drop_table_query,
        }

        query_func = operation_mapping.get(self.operation, None)

        if query_func is None:
            raise ValueError(f"Unsupported operation: {self.operation}")

        return query_func()

    def apply(self):
        raise NotImplementedError

    def revert(self):
        raise NotImplementedError

    def construct_add_table_query(self) -> ddl.CreateTable:
        return CreateTable(self.table)

    def construct_drop_table_query(self) -> ddl.DropTable:
        return DropTable(self.table)


class TableChange(Change):
    table: Table

    @field_validator("table", mode="before")
    def validate_table(cls, value):
        if not isinstance(value, Table):
            raise ValueError("Value must be an instance of sqlalchemy.Table")
        return value

    def __str__(self):
        return f"{str(self.table)} - {self.operation} - {self.status}"


class ColumnChange(Change):
    table: Table
    column: Column

    @model_validator(mode="before")
    def validate_table_and_column(cls, values):
        table = values.get("table")
        column = values.get("column")

        if not table or not column:
            raise ValueError("Both 'table' and 'column' must be provided")

        if table and not column:
            raise ValueError("Column must be provided when table is specified")

        return values

    def __str__(self):
        return f"{str(self.table)} - {self.operation} - {self.status}"
