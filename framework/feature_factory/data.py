from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column
from typing import List, Union
from framework.feature_factory import Helpers


class Joiner:
    def __init__(self, df: DataFrame, on, how: str):
        self.joining_df = df
        self.filter = on
        self.how = how


class DataSrc:
    def __init__(self, df: DataFrame, partitionCols:List[Union[str, Column]] = [], joiners: List[Joiner] = []):
        helpers = Helpers()
        self._df = df
        self._partition_cols = helpers._to_list(partitionCols)
        self._joiners = joiners

    def to_df(self):
        df = self._df
        for j in self._joiners:
            df.join(j.joining_df, on=j.filter, how=j.how)
        return df


