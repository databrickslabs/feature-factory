import unittest
from framework.feature_factory.feature import FeatureSet, Feature
import pyspark.sql.functions as F
from test.local_spark_singleton import SparkSingleton


class TestFeatureSet(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSingleton.get_instance()

    def test_add_feature(self):
        features = FeatureSet()
        assert len(features.features) == 0
        total_sales = Feature(_name="TOTAL_SALES",
                            _base_col=F.col("SELLING_RETAIL_AMT").cast("float"),
                            _filter=[],
                            _negative_value=0,
                            _agg_func=F.sum)
        features.add_feature(total_sales)
        assert len(features.features) == 1


    def tearDown(self):
        self.spark.stop()