from typing import List, Optional
from pydantic import BaseModel, Field
from ocelot.core.change import Change, OperationType
from io import StringIO
from colorama import Fore, Back, Style


class Diff(BaseModel):
    changes: Optional[List[Change]] = Field(default_factory=list)

    def add_change(self, change: Change):
        self.changes.append(change)

    def get_diff_msg(self):
        msgs = []
        reset = Style.RESET_ALL

        if self.changes:
            for change in self.changes:
                change_msg = StringIO()
                if change.operation == OperationType.ADD:
                    operation_subtext = f"{Fore.GREEN} + {change.operation.name}{reset}"
                elif change.operation == OperationType.DROP:
                    operation_subtext = f"{Fore.RED} + {change.operation.name}{reset}"
                else:
                    operation_subtext = ""

                changetype_subtext = f"({Fore.MAGENTA}{type(change).__name__}{reset})"
                table_subtext = f"{Fore.YELLOW}{str(change.table)}{reset}"

                change_msg.write(
                    f"{operation_subtext} - {table_subtext} {changetype_subtext}"
                )
                msgs.append(change_msg.getvalue())

        output_msg = "\n".join(msgs)

        return output_msg

    def __str__(self):
        return self.get_diff_msg()
