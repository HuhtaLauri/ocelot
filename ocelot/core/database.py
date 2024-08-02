from typing import List
from sqlalchemy import Table


class Database:
    def __init__(self):
        self.tables: List[Table] = []


    def compare_tables(self, df_2: 'Database'):
        """
        Compare the existing of tables by schema and tables
        with self and passed Database
        """

        raise NotImplementedError
        

if __name__ == "__main__":
    pass
