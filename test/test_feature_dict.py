import unittest
from framework.feature_factory.feature import Feature, FeatureSet, CompositeFeature
from framework.feature_factory.feature_dict import ImmutableDictBase
from framework.feature_factory import Feature_Factory
from framework.feature_factory.helpers import Helpers
import pyspark.sql.functions as f
import json
from pyspark.sql.types import StructType
from test.local_spark_singleton import SparkSingleton

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

        self._dct["total_quants"] = Feature(_name="total_quants",
                                           _base_col=f.col("ss_quantity"),
                                           _filter=[],
                                           _negative_value=None,
                                           _agg_func=f.sum)

        self._dct["total_sales"] = Feature(_name="total_sales",
                                           _base_col=f.col("ss_net_paid").cast("float"),
                                           _filter=self.valid_sales,
                                           _negative_value=0,
                                           _agg_func=f.sum)

        self._dct["sales_per_quants"] = (self.total_sales / self.total_quants).withName("sales_per_quants")


    @property
    def total_sales(self):
        return self._dct["total_sales"]

    @property
    def total_quants(self):
        return self._dct["total_quants"]

    @property
    def sales_per_quants(self):
        return self._dct["sales_per_quants"]

class TestFeatureDict(unittest.TestCase):
    def setUp(self):
        with open("test/data/sales_store_schema.json") as f:
            sales_schema = StructType.fromJson(json.load(f))
            self.sales_df = SparkSingleton.get_instance().read.csv("test/data/sales_store_tpcds.csv", schema=sales_schema, header=True)

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

    def test_composite_feature(self):
        helpers = Helpers()
        multiplier = helpers.get_categoricals_multiplier(self.sales_df, ["i_category"])
        features = StoreSales()
        fs = FeatureSet()
        fs.add_feature(features.total_sales)
        fs.add_feature(features.total_quants)
        cats_fs = fs.multiply(multiplier, "")
        ff = Feature_Factory()
        # df = ff.append_features(self.sales_df, [features.collector], [cats_fs])
        df = ff.append_features(self.sales_df, [features.collector], [fs, cats_fs])
        df.show()
        assert df.count() == self.sales_df.select("ss_customer_sk").distinct().count()

    def test_composite_feature_v2(self):
        helpers = Helpers()
        multiplier = helpers.get_categoricals_multiplier(self.sales_df, ["i_category"])
        features = StoreSales()
        fs_sales = FeatureSet()
        fs_sales.add_feature(features.total_sales)
        fs_quants = FeatureSet()
        fs_quants.add_feature(features.total_quants)
        cats_fs_sales = fs_sales.multiply(multiplier, "")
        cats_fs_quants = fs_quants.multiply(multiplier, "")
        sales_per_quants = cats_fs_sales / cats_fs_quants
        ff = Feature_Factory()
        df = ff.append_features(self.sales_df, [features.collector], 
            [fs_sales, fs_quants, cats_fs_sales, cats_fs_quants, sales_per_quants])
        # df.select("customer_id", "I_CATEGORY-HOME_SALES", "I_CATEGORY-HOME_QUANTS", "I_CATEGORY-HOME_SALES_per_I_CATEGORY-HOME_QUANTS").show()
        df.show()
        assert df.count() == self.sales_df.select("ss_customer_sk").distinct().count()
    
    def test_composite_feature_v3(self):
        helpers = Helpers()
        features = StoreSales()
        multiplier = helpers.get_categoricals_multiplier(self.sales_df, ["i_category"])
        sales_per_quants = CompositeFeature("sales_quants", features.total_sales, "/", features.total_quants)
        fs_result = sales_per_quants.multiply(multiplier, include_lineage=True)
        ff = Feature_Factory()
        df = ff.append_features(self.sales_df, [features.collector], 
            fs_result)
        df.show()
        print(f"the number of cols is: {len(df.columns)}")
        assert len(df.columns) == 33 # 10 categories for sales, quants, sales_per_quants, and customer_id, total_sales, total_quants
    
    def test_composite_feature_no_lineage(self):
        helpers = Helpers()
        features = StoreSales()
        multiplier = helpers.get_categoricals_multiplier(self.sales_df, ["i_category"])
        multipliable = FeatureSet()
        multipliable.add_feature(features.total_sales)
        multipliable.add_feature(features.total_quants)
        fs_multi = multipliable.multiply(multiplier, "")
        sales_per_quants = CompositeFeature("sales_quants", features.total_sales, "/", features.total_quants)
        fs_result = sales_per_quants.multiply(multiplier, include_lineage=False)
        ff = Feature_Factory()
        df = ff.append_features(self.sales_df, [features.collector], 
            [multipliable, fs_multi] + fs_result)
        df.show()
        print(f"the number of cols is: {len(df.columns)}")
        assert len(df.columns) == 33

    def test_sales_per_quants(self):
        helpers = Helpers()
        features = StoreSales()
        multiplier = helpers.get_categoricals_multiplier(self.sales_df, ["i_category"])
        sales_per_quants = features.sales_per_quants
        fs_result = sales_per_quants.multiply(multiplier, include_lineage=True)
        ff = Feature_Factory()
        df = ff.append_features(self.sales_df, [features.collector], 
            fs_result)
        df.show()
        print(f"the number of cols is: {len(df.columns)}")
        assert len(df.columns) == 33 # 10 categories for sales, quants, sales_per_quants, and customer_id, total_sales, total_quants

    def test_sales_per_quants_values(self):
        helpers = Helpers()
        features = StoreSales()
        df = SparkSingleton.get_instance().createDataFrame([(10.0, 2, 'dept1',1), (100.0, 10, 'dept2', 1)], ["ss_net_paid", "ss_quantity", "dept", "ss_customer_sk"])
        multiplier = helpers.get_categoricals_multiplier(df, ["dept"])
        sales_per_quants = features.sales_per_quants
        fs_result = sales_per_quants.multiply(multiplier, include_lineage=True)
        ff = Feature_Factory()
        result_df = ff.append_features(df, [features.collector], 
            fs_result)
        rs = result_df.collect()[0]
        assert rs["sales_per_quants_DEPT-DEPT1"] == 5.0 and rs["sales_per_quants_DEPT-DEPT2"] == 10.0

    def test_double_sales(self):
        helpers = Helpers()
        features = StoreSales()
        df = SparkSingleton.get_instance().createDataFrame([(10.0, 2, 'dept1',1), (100.0, 10, 'dept2', 1)], ["ss_net_paid", "ss_quantity", "dept", "ss_customer_sk"])
        multiplier = helpers.get_categoricals_multiplier(df, ["dept"])
        double_sales = CompositeFeature("double_sales", features.total_sales, "+", features.total_sales)
        fs_result = double_sales.multiply(multiplier, include_lineage=True)
        ff = Feature_Factory()
        result_df = ff.append_features(df, [features.collector], 
            fs_result)
        rs = result_df.collect()[0]
        assert rs["double_sales_DEPT-DEPT1"] == 20.0 and rs["double_sales_DEPT-DEPT2"] == 200.0

    def test_zero_sales(self):
        helpers = Helpers()
        features = StoreSales()
        multiplier = helpers.get_categoricals_multiplier(self.sales_df, ["i_category"])
        sales_zero = CompositeFeature("zero_sales", features.total_sales, "-", features.total_sales)
        fs_result = sales_zero.multiply(multiplier, include_lineage=True)
        ff = Feature_Factory()
        df = ff.append_features(self.sales_df, [features.collector], 
            fs_result)
        rs = df.select("zero_sales_I_CATEGORY-HOME","zero_sales_I_CATEGORY-SPORTS").collect()[0]
        assert rs["zero_sales_I_CATEGORY-HOME"] == 0.0 and rs["zero_sales_I_CATEGORY-SPORTS"] == 0.0
        print(f"the number of cols is: {len(df.columns)}")
        assert len(df.columns) == 22 # 10 categories for sales, sales-sales, and customer_id, total_sales

    def test_to_feature(self):
        features = StoreSales()
        sales_per_quants = features.sales_per_quants.to_feature()
        fs = FeatureSet()
        fs.add_feature(features.total_sales)
        fs.add_feature(features.total_quants)
        fs.add_feature(sales_per_quants)
        ff = Feature_Factory()
        df = ff.append_features(self.sales_df, [features.collector], [fs])
        # df.show()
        assert len(df.columns) == 4 # three added features and collector id


    def tearDown(self) -> None:
        pass