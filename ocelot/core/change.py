from pydantic import BaseModel, field_validator
from enum import Enum
from uuid import UUID, uuid4
from sqlalchemy.sql.schema import Table
from typing import Type


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

    def construct_change_query(self):
        pass

    def apply(self):
        raise NotImplementedError

    def revert(self):
        raise NotImplementedError


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
    table_id: str
    column_id: str
