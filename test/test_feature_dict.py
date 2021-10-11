import unittest
from framework.feature_factory.feature import Feature, FeatureSet
from framework.feature_factory.feature_dict import ImmutableDictBase
from framework.feature_factory import Feature_Factory
from framework.feature_factory.helpers import Helpers
import pyspark.sql.functions as f
import json
from pyspark.sql.types import StructType
from local_spark_singleton import SparkSingleton

class CommonFeatures(ImmutableDictBase):
    def __init__(self):
        self._dct["customer_id"] = Feature(_name="customer_id", _base_col=f.col("ss_customer_sk"))
        self._dct["trans_id"] = Feature(_name="trans_id", _base_col=f.concat("ss_ticket_number","d_date"))

    @property
    def collector(self):
        return self._dct["customer_id"]

    @property
    def trans_id(self):
        return self._dct["trans_id"]


class Filters(ImmutableDictBase):
    def __init__(self):
        self._dct["valid_sales"] = f.col("ss_net_paid") > 0

    @property
    def valid_sales(self):
        return self._dct["valid_sales"]


class StoreSales(CommonFeatures, Filters):
    def __init__(self):
        self._dct = dict()
        CommonFeatures.__init__(self)
        Filters.__init__(self)

        self._dct["total_trans"] = Feature(_name="total_trans",
                                           _base_col=self.trans_id,
                                           _filter=[],
                                           _negative_value=None,
                                           _agg_func=f.countDistinct)

        self._dct["total_sales"] = Feature(_name="total_sales",
                                           _base_col=f.col("ss_net_paid").cast("float"),
                                           _filter=self.valid_sales,
                                           _negative_value=0,
                                           _agg_func=f.sum)

    @property
    def total_sales(self):
        return self._dct["total_sales"]

    @property
    def total_trans(self):
        return self._dct["total_trans"]

class TestFeatureDict(unittest.TestCase):
    def setUp(self):
        with open("./data/sales_store_schema.json") as f:
            sales_schema = StructType.fromJson(json.load(f))
            self.sales_df = SparkSingleton.get_instance().read.csv("./data/sales_store_tpcds.csv", schema=sales_schema, header=True)

    def test_feature_dict(self):
        helpers = Helpers()
        multiplier = helpers.get_categoricals_multiplier(self.sales_df, ["i_category"])
        features = StoreSales()
        fs = FeatureSet()
        fs.add_feature(features.total_sales)
        cats_fs = fs.multiply(multiplier, "")
        ff = Feature_Factory()
        df = ff.append_features(self.sales_df, [features.collector], [cats_fs])
        df.show()
        assert df.count() == self.sales_df.select("ss_customer_sk").distinct().count()

    def tearDown(self) -> None:
        pass