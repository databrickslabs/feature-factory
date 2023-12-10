from framework.channel import Channel
from .sales import Sales
from featurefamily_common.trends import TrendsCommon
from framework.feature_factory.dtm import DateTimeManager
from framework.feature_factory import Helpers
from framework.spark_singleton import SparkSingleton
from framework.configobj import ConfigObj
import sys, traceback, logging
from featurefamily_common.groupbys import GroupByCommon
from framework.feature_factory.data import Joiner
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)

spark = SparkSingleton.get_instance()


class Store(Channel):
    def __init__(self, _snapshot_date=None, _config: ConfigObj = ConfigObj()):
        self.dtm = DateTimeManager(_snapshot_date=_snapshot_date,
                                   _dt_col="d_date",
                                   _dt_format="%Y-%m-%d %H:%M:%S",
                                   _date_format="%Y-%m-%d",
                                   _config=_config,
                                   _partition_col="p_yyyymm",
                                   _partition_dt_format="%Y%m")
        self.dtm.append_periods(["1m", "3m", "6m", "12m"])
        self.config = self.dtm.get_config()
        Channel.__init__(self, "Store", self.dtm, self.config)
        self._create_data_source()
        self.sales = Sales(self.config)
        # self.groupby = Store._GroupBy(self)

    def Sales(self):
        self.sales = Sales(self.config)
        return self.sales

    def Trends(self, featureSet_to_trend, trend_ranges, _dtm=None, _config: ConfigObj = None):
        """
        Trends is a method that constructs the TrendsCommon generic FeatureFamily. It returns features, slope and y-intercept
        such that the user can understand the trend line and ideally future values for various features.
        :param featureSet_to_trend: A feature set to be trended
        :param trend_ranges: list of ranges to be trended [['1m','12m'], ['1w', '4w']]. Accepted range periods are
        d = day
        w = week
        m = month
        y = year
        :param _dtm: A specific dtm can be passed in but generally the dtm from self should be used
        :param _config: A specific configObj can be passed in but generally the config is taken from self
        :return:
        """
        if _config is None:
            time_helpers = self.config.get_config("time_helpers")
        else:
            time_helpers = _config.get_config("time_helpers")

        dtm = self.dtm if _dtm is None else _dtm
        trends = TrendsCommon(featureSet_to_trend, trend_ranges, dtm, time_helpers)
        return trends

    def _create_data_source(self):
        try:
            item_df = spark.read.table("tomes_tpcds_delta_1tb_item")
            store_df = spark.read.table("tomes_tpcds_delta_1tb_store")
            # date_df = spark.read.table('tomes_tpcds_delta_1tb.date_dim')

            store_sales_df = spark.read.table("tomes_tpcds_delta_1tb_store_sales_enhanced")
            # store_returns_df = spark.read.table("tomes_tpcds_delta_1tb.store_returns_enhanced")

            store_profit_by_div_df = store_sales_df.join(
                store_df.withColumnRenamed("s_store_sk", "ss_store_sk"),
                ["ss_store_sk"]
            ) \
            .groupBy(F.col("s_division_id")).agg(F.sum(F.col("ss_net_profit")).alias("net_profit_by_div"))
            joiner1 = Joiner(store_profit_by_div_df, on=["s_division_id"], how="left")
            joiner2 = Joiner(store_df, on=F.col("ss_store_sk") == F.col('s_store_sk'), how="inner")

            self.add_source("store_sales", store_sales_df, ["p_yyyymm"], joiners=[joiner1, joiner2])
            self.add_source("store", store_df)
            self.add_source("item", item_df)

        except Exception as e:
            logger.warning("Error loading default sources. {}".format(str(e)))
            traceback.print_exc(file=sys.stdout)

        return self.sources

