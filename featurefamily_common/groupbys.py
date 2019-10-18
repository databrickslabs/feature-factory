import pyspark.sql.functions as F


class GroupByCommon:
    def __init__(self):
        pass

    def get(self):
        groupby = self._groupby_cols
        self._groupby_cols = []
        return groupby
