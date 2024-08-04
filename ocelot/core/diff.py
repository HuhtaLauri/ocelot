from typing import List, Optional
from pydantic import BaseModel, Field
from ocelot.core.change import Change


class Diff(BaseModel):
    changes: Optional[List[Change]] = Field(default_factory=list)

    def add_change(self, change: Change):
        self.changes.append(change)
