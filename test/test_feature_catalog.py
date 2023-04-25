import unittest
from framework.feature_factory.feature import Feature, FeatureSet, CompositeFeature
from framework.feature_factory.feature_dict import ImmutableDictBase
from framework.feature_factory import Feature_Factory
from framework.feature_factory.helpers import Helpers
import pyspark.sql.functions as f
import json
from pyspark.sql.types import StructType
from test.local_spark_singleton import SparkSingleton

class SalesCatalog:
    _valid_sales_filter = f.col("ss_net_paid") > 0

    total_sales = Feature.create(base_col=f.col("ss_net_paid").cast("float"),
                                           filter=_valid_sales_filter,
                                           agg_func=f.sum)
    total_quants = Feature.create(base_col=f.col("ss_quantity"),
                                           agg_func=f.sum)


class TestSalesCatalog(unittest.TestCase):
    def setUp(self):
        with open("test/data/sales_store_schema.json") as f:
            sales_schema = StructType.fromJson(json.load(f))
            self.sales_df = SparkSingleton.get_instance().read.csv("test/data/sales_store_tpcds.csv", schema=sales_schema, header=True)

    def test_append_catalog(self):
        customer_id = f.col("ss_customer_sk").alias("customer_id")
        ff = Feature_Factory()
        df = ff.append_catalog(self.sales_df, [customer_id], SalesCatalog)
        assert df.count() > 0
        assert "total_sales" in df.columns and "total_quants" in df.columns
