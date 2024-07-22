from pydantic import BaseModel
from enum import Enum
from uuid import UUID, uuid4


class ChangeStatus(Enum):
    PENDING = "pending"
    APPLIED = "applied"
    ROLLED_BACK = "rolled_back"

    def __str__(self):
        return self.name


class Change(BaseModel):
    id: UUID = uuid4()
    operation: str
    column: str
    status: ChangeStatus = ChangeStatus.PENDING
