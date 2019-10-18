from pyspark.sql.functions import col, lit, when, struct
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from framework.feature_factory.feature import Feature
from framework.feature_factory.feature_family import FeatureFamily
from framework.feature_factory import Helpers
from framework.configobj import ConfigObj
import inspect

joiner_func = Helpers()._register_joiner_func()
multipliable = Helpers()._register_feature_func()
base_feat = Helpers()._register_feature_func()


# Extend FeatureFamily
class Sales(FeatureFamily):
    def __init__(self, config=ConfigObj()):
        self._multipliable_feat_func = multipliable
        self._base_feat_func = base_feat
        self._joiner_func = joiner_func
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

    @joiner_func
    def join_product_item_dim(self):
        data_set_join_key = "joiners.sales.item"
        if not self.config.contains(data_set_join_key):
            conf = {"target_join_df": 'sources.item',
                    "filter_condition": F.col('i_item_sk') == F.col("ws_item_sk"),  # Join condition
                    "join_type": "inner",
                    "optimizer": ""}
            self.config.add(data_set_join_key, conf)
        else:
            print("sources.item is already loaded to create join_product_item_dim.")

    @joiner_func
    def join_web_inventory_dim(self):
        data_set_join_key = "joiners.sales.inventory"
        if not self.config.contains(data_set_join_key):
            conf = {'target_join_df': 'sources.inventory',
                    'filter_condition': F.col("ws_warehouse_sk") == F.col('inv_warehouse_sk'),
                    'join_type': 'inner',
                    'optmizer': ""}
            self.config.add(data_set_join_key, conf)
        else:
            print("sources.item is already loaded to create join_product_item_dim.")

    @joiner_func
    def join_store_date_dim(self):
        data_set_join_key = "joiners.sales.date"
        if not self.config.contains(data_set_join_key):
            conf = {'target_join_df': 'sources.date',
                    'filter_condition': F.col("ws_sold_date_sk") == F.col('d_date_sk'),
                    'join_type': 'inner',
                    'optmizer': "broadcast"}
            self.config.add(data_set_join_key, conf)
        else:
            print("sources.item is already loaded to create join_product_item_dim.")
