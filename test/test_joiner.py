import unittest
from framework.feature_factory import Helpers
from framework.feature_factory.data import DataSrc, Joiner
from test.local_spark_singleton import SparkSingleton


class TestJoiner(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSingleton.get_instance()
        self.helpers = Helpers()

    def test__joiners(self):
        trans_df = self.spark.createDataFrame([(1, 100, "item1"), (2, 200, "item2")], ["trans_id", "sales", "item_id"])
        dim_df = self.spark.createDataFrame([("item1", "desc1"), ("item2", "desc2")], ["item_id", "item_desc"])
        joiner = Joiner(dim_df, on=["item_id"], how="inner")
        src_df = DataSrc(trans_df, joiners=[joiner]).to_df()
        assert len(src_df.columns) == 3

    def tearDown(self):
        self.spark.stop()