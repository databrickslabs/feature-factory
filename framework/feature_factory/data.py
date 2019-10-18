from pyspark.sql.dataframe import DataFrame
# from framework.feature_factory import Helpers


class Data:
    # convert str to col automatically
    def __init__(self, table: DataFrame, partition_cols=[]):
        self.df = table
        self.partition_cols = partition_cols
        self.joined_tables = []

