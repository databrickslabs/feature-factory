from framework.feature_factory.feature_family import FeatureFamily
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, struct
from framework.feature_factory.feature import Feature
from framework.feature_factory.helpers import Helpers
from framework.configobj import ConfigObj
import inspect
from datetime import date


feat_func = Helpers()._register_feature_func()
joiner_func = Helpers()._register_joiner_func()


# ADD DOCS HERE

class SalesCommon:
    def __init__(self, config=ConfigObj()):
        # FeatureFamily.__init__(self, config)
        self.config = config
        self._joiner_func = joiner_func
        self._feat_func = feat_func
        # FeatureFamily.__init__(self, config)

    def transfer_features(self, cls):
        for fn, func in self._feat_func.all.items():
            setattr(cls, fn, func)

    # @feat_func
    # def totalAmrmEarned(self,
    #                     _name="total_gross_sales",
    #                     _base_col="gross_sales",
    #                     _filter=[],
    #                     _negative_value=0,
    #                     _agg_func=F.sum,
    #                     _agg_alias=None):
    #
    #     self._create_feature(inspect.currentframe())
    #     return self