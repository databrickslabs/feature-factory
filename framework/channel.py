from framework.feature_factory import Feature_Factory
from framework.feature_factory.data import DataSrc
from framework.feature_factory.dtm import DateTimeManager
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame
from framework.configobj import ConfigObj
from framework.feature_factory.helpers import Helpers
from collections import OrderedDict
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from framework.feature_factory.feature import Multiplier
import sys


class Channel:
    """
    Base class for a channel
    """
    def __init__(self, _name, _dtm: DateTimeManager,
                 _config: ConfigObj=ConfigObj()):
        """

        :param _name:
        :param _partition_start: to be applied as a filter to the partition_cols of all sources/cores of this partner.
            The partition_cols is an argument for adding a data source/core to a partner. e.g. partition_start can be [201706]
            when add the core: channel.add_core("sales_item_fact", core_df, "P_TRANS_MONTH_ID")
        :param _partition_end: to be applied as a filter to the partition_cols of all sources/cores of this partner.
            The partition_cols is an argument for adding a data source/core to a partner. e.g. partition_start can be [201706]
            when add the core: channel.add_core("sales_item_fact", core_df, "P_TRANS_MONTH_ID")
        :param _config:
        """
        self.dtm = _dtm
        self.config = _config
        self.channel_name = _name

        # Derive final snapshot and add it config
        self.snapshot_date = _dtm.snapshot_date
        self.snapshot_date_dt = _dtm.snapshot_date_dt

        # TODO - Improve partitions to handle nested partitions
        # Get the partition if it exists
        self.partition_start = self.config.get_or_else("time_helpers.partition_lower",
                                                   (datetime.today() - relativedelta(years=1))
                                                   .strftime(_dtm.partition_dt_format))
        self.partition_end = self.config.get_or_else("time_helpers.partition_upper",
                                                 datetime.today().strftime(_dtm.partition_dt_format))
        self.groupby_cols = self.config.get_or_else("_groupby_cols", [])

        self._features = OrderedDict()
        if not self.config.contains("cores"):
            self.config.add("cores", {})
        if not self.config.contains("sources"):
            self.config.add("sources", {})
        # self.cores = self.config.get_or_else("cores", None)
        self.sources = self.config.get_or_else("sources", None)

        self.ff = Feature_Factory()
        # self._populate_partition_range()
        self.helpers = Helpers()

    def _get_groupby_cols(self):
        """
        Returns a list of cols for the gorupBy operation on the sources/cores.
        :return:
        """
        return self.groupby_cols

    # def list_cores(self):
    #     """
    #     Returns a list of keys of cores.
    #     :return:
    #     """
    #     return list(self.cores.keys())

    def list_sources(self):
        """
        Returns a list of keys of sources.
        :return:
        """
        return list(self.sources.keys())

    # def get_core(self, name: str):
    #     """
    #     Gets the dataframe of a core by name.
    #     :param name:
    #     :return:
    #     """
    #     if name in self.cores:
    #         return self._apply_metric_filters(name, self.cores[name].df)
    #     else:
    #         return None

    def get_source(self, name: str):
        """
        Gets the dataframe of a source by name.
        :param name:
        :return:
        """
        if name in self.sources:
            return self._apply_metric_filters(name, self.sources[name].df)
        else:
            return None

    def get_data(self, df_path):
        """
        Get dataframe from cores or sources using path such as "sources.collector_dim"
        :param path:
        :return:
        """
        df_parts = [p.strip() for p in df_path.split(".")]
        cores = self.config.get_or_else("cores", {})
        sources = self.config.get_or_else("sources", {})
        data_source = cores if df_parts[0] == "cores" else sources
        if df_parts[1] in data_source:
            return data_source[df_parts[1]].to_df()
        else:
            return None

    def _apply_metric_filters(self, name: str, df: DataFrame):
        metric_filters = self.config.get_config("metric_filters")
        metric_filter = metric_filters.get_or_else(name, None)
        if metric_filter is not None:
            df.filter(metric_filter)
        return df

    # def add_core(self, name: str, table: DataFrame, partition_col=[]):
    #     """
    #     Adds a core to the partner.
    #     :param name:
    #     :param table: the dataframe of the core
    #     :param partition_col: the columns to be filtered using partition_start and partition_end
    #     :return:
    #     """
    #
    #     self._add_data(self.cores, name, table, partition_col)

    def add_source(self, name: str, table: DataFrame, partition_col=[], joiners=[]):
        """
        Adds a source to the partner.
        :param name:
        :param table: the dataframe of the source
        :param partition_col: the columns to be filtered using partition_start and partition_end
        :return:
        """
        self._add_data(self.sources, name, table, partition_col, joiners)

    def _add_data(self, datalist: dict, name: str, table: DataFrame, partition_cols=[], joiners=[]):
        # if name not in datalist:

        if len(partition_cols) > 0:
            p_filter = self.dtm.scoped_partition_filter(
                start=self.partition_start,
                end=self.partition_end,
                partition_col=partition_cols[0],
                input_fmt=self.dtm.partition_dt_format)
            d = DataSrc(table.filter(p_filter), partition_cols, joiners)
        else:
            d = DataSrc(table, partition_cols, joiners)

        # TODO - Add this back in to support nested partition columns
        # TODO - But it will require a few tweaks
        # if len(partition_cols) > 0 and len(self.partition_start) > 0:
        #     tf_filters = [col(tfc) >= self.partition_start[i] for i, tfc in enumerate(d.partition_cols)]
        #     where_clause = tf_filters[0]
        #     for f in tf_filters[1:]:
        #         where_clause &= f
        #     print("Applying filter {} to dataframe {}".format(where_clause, name))
        #     d.df = d.df.filter(where_clause)
        #
        # if len(partition_cols) > 0 and len(self.partition_end) > 0:
        #     tf_filters = [col(tfc) <= self.partition_end[i] for i, tfc in enumerate(d.partition_cols)]
        #     where_clause = tf_filters[0]
        #     for f in tf_filters[1:]:
        #         where_clause &= f
        #     print("Applying filter {} to dataframe {}".format(where_clause, name))
        #     d.df = d.df.filter(where_clause)

        datalist[name] = d

    # def remove_core(self, name: str):
    #     self.config.drop("cores.{}".format(name))


    def remove_source(self, name: str):
        self.config.drop("sources.{}".format(name))

    def get_daterange_multiplier(self, time_helpers: ConfigObj = None):
        time_helpers = self.config.get_config("time_helpers") if time_helpers is None else time_helpers
        return Multiplier._create_from_daterange(self.dtm, time_helpers)

    # def _create_groupby(self, joiner_key: str, groupby_col, assign_df: bool=True):
    #     joiner_config = self.config.get_or_else(joiner_key, dict())
    #     if assign_df:
    #         table_name = self.helpers._get_joiner_key(joiner_key)
    #         joiner_config["target_join_df"] = self.get_source(table_name)
    #     return {"col": groupby_col, "joiner": joiner_config, "joiner_key": joiner_key}
