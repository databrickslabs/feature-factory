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
    def netStoreSales(self,
                      _name="net_sales",
                      _base_col='ss_net_profit',  ##how does it know where to pull this column from?
                      _filter=[F.col('ss_net_profit') > 0],
                      _negative_value=0,
                      _agg_func=F.sum):  # by default features are multipliable
        self._create_feature(inspect.currentframe())
        return self

    @multipliable
    def totalQuantity(self,
                      _name="total_quantity",
                      _base_col='ss_quantity',  ##how does it know where to pull this column from?
                      _filter=[F.col('ss_quantity') > 0],
                      _negative_value=0,
                      _agg_func=F.sum):  # by default features are multipliable
        self._create_feature(inspect.currentframe())
        return self

    @multipliable
    def netSalesPerQuant(self,
                         _name='net_sales_per_quantity',
                         _base_col=F.col('net_sales') / F.col('total_quantity'),
                         _filter=[],
                         _negative_value=0,
                         _agg_func=F.sum):
        self.netStoreSales().totalQuantity()
        self._create_feature(inspect.currentframe())
        return self

    # Demonstrating how to use joiners and base features here
    @base_feat
    def divisionSales(self,
                      _name='net_profit_for_division',
                      _base_col='net_profit_by_div',
                      _negative_value=0,
                      _agg_func=F.sum):
        f = self._create_feature(inspect.currentframe())
        f._add_joiner("joiners.sales.store", self.config)
        f._add_joiner("joiners.sales.store_net_profit_by_division", self.config)
        return self

    # Complex Joiner
    # This is an example of a complex joiner but it's usually better to create to main dfs and join
    # Them to add these kinds of features (ones that require an agg at a different level)
    @joiner_func
    def join_store_net_profit_by_division(self):
        data_set_join_key = "joiners.sales.store_net_profit_by_division"
        if not self.config.contains(data_set_join_key):
            store_df: DataFrame = F.broadcast(self.config.get_or_else("sources.store", None).df)
            store_sales_df: DataFrame = self.config.get_or_else("cores.store_sales", None).df
            store_profit_by_div_df = store_sales_df.join(store_df.withColumnRenamed("s_store_sk", "ss_store_sk"), ["ss_store_sk"])\
                .groupBy(F.col("s_division_id")).agg(F.sum(F.col("ss_net_profit")).alias("net_profit_by_div"))
            conf = {'target_join_df': store_profit_by_div_df,
                    'filter_condition': ["s_division_id"],
                    'join_type': 'left',
                    'optmizer': "broadcast"}
            self.config.add(data_set_join_key, conf)
        else:
            print("sources.store is already loaded to create join_store_division_dim.")

    # Simple Joiner
    # If the feature simply needs data from another table, specify the join logic here.
    @joiner_func
    def join_store(self):
        data_set_join_key = "joiners.sales.store"
        if not self.config.contains(data_set_join_key):
            conf = {'target_join_df': 'sources.store',
                    'filter_condition': F.col("ss_store_sk") == F.col('s_store_sk'),
                    'join_type': 'inner',
                    'optmizer': "broadcast"}
            self.config.add(data_set_join_key, conf)
        else:
            print("sources.store is already loaded to create join_store_location_dim.")
