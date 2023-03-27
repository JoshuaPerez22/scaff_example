from pyspark.sql import Column
from pyspark.sql.functions import col


class Field:

    def __init__(self, name):
        self.name = name

    def __call__(self) -> Column:
        return col(self.name)
