from pyspark.sql.functions import col, lit, when, struct
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from framework.feature_factory.feature import Feature
from framework.feature_factory.feature_family import FeatureFamily
from framework.feature_factory import Helpers
from framework.configobj import ConfigObj
from framework.spark_singleton import SparkSingleton
import inspect

# joiner_func = Helpers()._register_joiner_func()
multipliable = Helpers()._register_feature_func()
base_feat = Helpers()._register_feature_func()

spark = SparkSingleton.get_instance()

# Extend FeatureFamily
class Sales(FeatureFamily):
    def __init__(self, config=ConfigObj()):
        self._multipliable_feat_func = multipliable
        self._base_feat_func = base_feat
        # self._joiner_func = joiner_func
        # self._build_all()
        FeatureFamily.__init__(self, config)

    @multipliable
    def netWebSales(self,
                    _name="net_web_sales",
                    _base_col='ws_net_paid',  ##how does it know where to pull this column from?
                    _filter=[F.col('ws_net_paid') > 0],
                    _negative_value=0,
                    _agg_func=F.sum):  # by default features are multipliable
        self._create_feature(inspect.currentframe())
        return self

    @multipliable
    def avgListPrice(self,
                     _name="avg_list_price",
                     _base_col='ws_list_price',  ##how does it know where to pull this column from?
                     _filter=[],
                     _negative_value=0,
                     _agg_func=F.avg):  # by default features are multipliable
        self._create_feature(inspect.currentframe())
        return self
