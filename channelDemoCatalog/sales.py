from pyspark.sql.functions import col, lit, when, struct
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from framework.feature_factory.feature import Feature
from framework.feature_factory.feature_family import FeatureFamily
from framework.feature_factory import Helpers
from framework.configobj import ConfigObj
import inspect

# joiner_func = Helpers()._register_joiner_func()
multipliable = Helpers()._register_feature_func()
base_feat = Helpers()._register_feature_func()


# Extend FeatureFamily
class Sales(FeatureFamily):
    def __init__(self, config=ConfigObj()):
        self._multipliable_feat_func = multipliable
        self._base_feat_func = base_feat
        # self._build_all()
        FeatureFamily.__init__(self, config)

    @multipliable
    def netCatalogPaid(self,
                       _name="net_paid",
                       _base_col='cs_net_paid',
                       _filter=[F.col('cs_net_paid') > 0],
                       _negative_value=0,
                       _agg_func=F.sum):  # by default features are multipliable
        self._create_feature(inspect.currentframe())
        return self

    @multipliable
    def netCatalogProfit(self,
                         _name="net_profit",
                         _base_col='cs_net_profit',  ##how does it know where to pull this column from?
                         _filter=[],
                         _negative_value=0,
                         _agg_func=F.sum):  # by default features are multipliable
        self._create_feature(inspect.currentframe())
        return self
