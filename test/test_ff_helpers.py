import unittest
from framework.feature_factory import Helpers
from framework.feature_factory.feature import Feature
import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from framework.spark_singleton import SparkSingleton
import re

class TestFeatureFactoryHelpers(unittest.TestCase):

    def setUp(self):
        data_base_dir = "test/data"
        self.spark = SparkSingleton.get_instance()
        self.helpers = Helpers()
        self.item_df = self.spark.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_item.csv", inferSchema=True,
                                       header=True)

    def test_to_list(self):
        l = self.helpers._to_list(["col1", "col2", "col3"])
        assert len(l) == 3, "A list input should return the same list"

        l = self.helpers._to_list({"col1": "col1", "col2": "col2"})
        assert len(l) == 2, "A dict input should return a list"

        l = self.helpers._to_list("col1")
        assert len(l) == 1, "a single string will be wrapped up as a list of col."

    def test_dedup_tests(self):
        df = self.spark.createDataFrame([(1,2,3)], ["col1", "col2", "col3"])
        f1 = Feature(_name="col1", _base_col=F.col("col1"))
        f4 = Feature(_name="col4", _base_col=F.col("col4"), _filter=F.col("col4").between(F.lit(10), F.lit(100)), _agg_func=F.approx_count_distinct)
        f4_dup = Feature(_name="col4_dup", _base_col=F.col("col4"), _filter=F.col("col4").between(F.lit(10), F.lit(100)), _agg_func=F.approx_count_distinct)
        features = [f1, f4, f4_dup]
        dedupped_features, dup_features = self.helpers._dedup_features(df, features)
        assert len(dedupped_features) == 1, "dedupped features should has one feature."
        assert len(dup_features) ==2, "col1, col4 is duplicated."

    def test_dedup_perf(self):
        df = self.spark.createDataFrame([(1,2,3)], ["col1", "col2", "col3"])
        features = [Feature(_name="col{}".format(i), _base_col=F.col("col{}".format(i))) for i in range(1000)]
        dedupped_features, dup_features = self.helpers._dedup_fast(df, features)
        print(dup_features)

    def test_regex(self):
        alias = " Special $#! characters   spaces 888323 "
        cleaned_name = ''.join(ch for ch in alias if ch.isalnum() or ch in ['-', '_'])
        assert cleaned_name == "Specialcharactersspaces888323", "special characters should be removed."

    def test_validate_col(self):
        df = self.spark.createDataFrame([(1,2,3)], ["col1", "col2", "col3"])
        col = F.col("col1")
        result, undef_cols = self.helpers._validate_col(df, col, F.lit(1))
        assert result, "col1 should be defined in df."
        result, undef_cols = self.helpers._validate_col(df, col, F.col("col4"))
        assert not result, "{} are not defined in df.".format(undef_cols)

    def test_col_equals(self):
        feature_filter1 = F.col("P_TRANS_MONTH_ID").between(F.lit(10), F.lit(100))
        feature_filter2 = F.col("P_TRANS_MONTH_ID").between(F.lit(20), F.lit(200))

        feature1 = Feature(_name="Channel_NbOfVisitedStores",
                _base_col=F.col("STORE_ID"),
                _filter=feature_filter1,
                _negative_value="",
                _agg_func=F.approx_count_distinct
                )

        feature2 = Feature(_name="Channel_NbOfVisitedStores",
                           _base_col=F.col("STORE_ID"),
                           _filter=feature_filter2,
                           _negative_value="",
                           _agg_func=F.approx_count_distinct
                           )

        col1 = feature1.assembled_column
        col2 = feature2.assembled_column
        # # col1 = feature1.base_col
        # # col2 = feature2.base_col
        col1_str = col1._jc.toString()
        col2_str = col2._jc.toString()
        # equals = col1._jc.equals(col2._jc)
        equals = col1_str == col2_str
        assert not equals, "error comparing two columns"


    def test_get_approx_distinct_count_for_col(self):
        approx_cnt = self.helpers._get_approx_distinct_count_for_col(self.item_df, "i_item_id")
        cnt = self.item_df.select("i_item_id").distinct().count()
        assert float(cnt - approx_cnt)/cnt <= 0.05

    def tearDown(self):
        self.spark.stop()