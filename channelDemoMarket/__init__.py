from framework.channel import Channel
from channelDemoMarket.sales import Sales
from featurefamily_common.trends import TrendsCommon
from framework.feature_factory.dtm import DateTimeManager
from framework.feature_factory import Helpers
from framework.spark_singleton import SparkSingleton
from framework.configobj import ConfigObj
import sys, traceback, logging
from featurefamily_common.groupbys import GroupByCommon
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

spark = SparkSingleton.get_instance()


class Store(Channel):
    def __init__(self, _snapshot_date=None, _config: ConfigObj = ConfigObj()):
        self.dtm = DateTimeManager(_snapshot_date=_snapshot_date, #Upper date limit to be included?
                                   _dt_col="header_capture_month",
                                   _dt_format="%Y-%m-%d %H:%M:%S",
                                   _date_format="%Y-%m-%d", #Format of the input snapshot date!
                                   #_date_format="%Y-%m-%d %H:%M:%S",
                                   _config=_config,
                                   #_partition_col="p_yyyymm",
                                   _partition_dt_format="%Y%m"
                                   )
        self.dtm.append_periods(["1m", "3m"])
        self.config = self.dtm.get_config()
        Channel.__init__(self, "Store", self.dtm, self.config)
        self._create_default_cores()
        self._create_default_sources()
        self.sales = Sales(self.config)
        self.groupby = Store._GroupBy(self)

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

    def _create_default_cores(self):
        try:
            df = spark.read.csv("market_data.csv", inferSchema=True, header=True)
            self.add_core("issuer", df, []) #!do not use p_yyyymm here.
            #df = spark.read.csv("market_id.csv", inferSchema=True, header=True)
            #self.add_core("bank_id", df, ["p_yyyymm"])
        except Exception as e:
            logger.warning("Error loading default cores. {}".format(str(e)))
            traceback.print_exc(file=sys.stdout)

        return self.cores

    def _create_default_sources(self):
        try:
            pass
        except Exception as e:
            logger.warning("Error loading default sources. {}".format(str(e)))
            traceback.print_exc(file=sys.stdout)

        return self.sources

    class _GroupBy(GroupByCommon):
        def __init__(self, _store):
            self.helpers = Helpers()
            self._groupby_cols = []
            self._store = _store

        def store(self):
            """
                GroupBy TRANSACTION_DATE and name column 'TRANS_DATE'
            :return:
            """
            self._groupby_cols.append(F.col("ss_store_sk").alias("STORE_ID"))
            return self
