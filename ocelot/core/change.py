from pydantic import BaseModel
from enum import Enum
from uuid import UUID, uuid4


class ChangeStatus(Enum):
    PENDING = "pending"
    APPLIED = "applied"
    ROLLED_BACK = "rolled_back"

    def __str__(self):
        return self.name


class Operation(Enum):
    ADD = "add"
    DROP = "drop"
    ALTER = "alter"


class Change(BaseModel):
    id: UUID = uuid4()
    operation: Operation
    status: ChangeStatus = ChangeStatus.PENDING

    def apply(self):
        raise NotImplementedError

    def revert(self):
        raise NotImplementedError


class TableChange(Change):
    table_id: str

    def __str__(self):
        return f"{self.table_id} - {self.operation} - {self.status}"


class ColumnChange(Change):
    table_id: str
    column_id: str
