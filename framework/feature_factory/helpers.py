from pyspark.sql.functions import col, lit, when, struct
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from framework.feature_factory.feature import Feature
from framework.spark_singleton import SparkSingleton
from framework import feature_factory

from datetime import datetime
from datetime import timedelta
import inspect
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)
spark = SparkSingleton.get_instance()


class Helpers:
    def __init__(self):
        pass

    def _get_approx_distinct_count_for_col(self, df: DataFrame, _dcol: str, _rsd=0.05):
        return df.select(F.approx_count_distinct(col(_dcol), rsd=_rsd)) \
            .rdd.map(lambda row: row[0]).collect()[0]

    def _get_cat_feature_val_col(self, agg_col):
        if isinstance(agg_col, int):
            return lit(agg_col)
        elif isinstance(agg_col, str):
            return col(agg_col)
        else:
            return agg_col

    def get_time_range_in_days(self, time_string: str):
        """
        Generates epoch seconds for a time string
        supported time range strings are
        m : minute
        h : hour
        d : day
        M : Month
        y : year
        :param time_string: string of integer + string such as 3M for 3 months
        :return:
        """
        i = int(time_string[:len(time_string) - 1])
        time_type = str(time_string[len(time_string) - 1:])
        switch = {
            "m": 60,
            "h": 3600,
            "d": 86400,
            "M": 86400 * 30,
            "y": 86400 * 30 * 365
        }
        return i * switch.get(time_type)

    def _convert_daterange_date(self, date_range, format):
        start_date = datetime.strptime(str(date_range["start"]), format).date()
        end_date = datetime.strptime(str(date_range["end"]), format).date()
        return (start_date, end_date)

    def get_categoricals_multiplier(self, df: DataFrame, col_list: list=[], ignore_cols: list=[], approx_distinct=100, rsd=0.05):
        """
        Gets a dictionary of col names and the distinct values in the column.
        :param df:
        :param col_list: Subset list of columns to use as categoricals; if null, all columns will be checked for
        approx_distinct values and considered categoricals
        :param ignore_cols: when not selecting a subset of columns using col_list, ignore columns is a list of
        columns that will be skipped when searching for categoricals with approx_distinct columns.
        :param approx_distinct: log a warning message if the approx number of distinct values is greater than this threshold.
        :param rsd:
        :return:
        """
        # TODO - Add logging of findings
        filter_vals = []
        filter_cols = col_list

        if not col_list:
            for (dcol, dtype) in df.drop(*ignore_cols).dtypes:
                if dtype == 'string':
                    if self._get_approx_distinct_count_for_col(df, dcol, _rsd=rsd) <= approx_distinct:
                        # LOG print("{} has approx {} distincts".format(dcol, cnt))
                        # LOG print("appending {}".format(dcol))
                        filter_vals.append(df.select(col(dcol)) \
                                           .filter((col(dcol).isNotNull()) &
                                                   (col(dcol).isin("", "Y", "N") == False)) \
                                           .distinct().rdd.map(lambda row: str(row[0])).collect())
                        filter_cols.append(dcol)
            # ?? TODO - What about the rest of the potential categorical types (i.e. bools/ints/floats/etc)
            return feature_factory.feature.Multiplier._create_from_cats(filter_cols, filter_vals)
        else:
            for dcol in col_list:
                if self._get_approx_distinct_count_for_col(df, dcol) > approx_distinct:
                    print("WARN! {} has more than {} distinct values".format(dcol, approx_distinct))
                filter_vals.append(df.select(col(dcol)) \
                                   .filter((col(dcol).isNotNull()) &
                                           (col(dcol).isin("", "Y", "N") == False)) \
                                   .distinct().rdd.map(lambda row: str(row[0])).collect())
            return feature_factory.feature.Multiplier._create_from_cats(filter_cols, filter_vals)

    def _clean_alias(self, alias: str):
        """
        Escapes all special characters except - and _
        :param alias:
        :return:
        """
        return ''.join(ch for ch in alias if ch.isalnum() or ch in ['-', '_']).strip('_-')

    def _to_list(self, items):
        """
        Converts items of string, dict or list to a list of columns.
        if items is string, assume the string is comma separated.
        :param items:
        :return: a list of columns
        """
        return Converter(items, F.col).list

    # def _extract_groupby_joiner(self, groupby_cols):
    #     groupbys = []
    #     joiners = {}
    #     for g in groupby_cols:
    #         if isinstance(g, dict):
    #             groupbys.append(g["col"])
    #             joiners[g["joiner_key"]] = g["joiner"]
    #         else:
    #             item = F.col(g) if isinstance(g, str) else g
    #             groupbys.append(item)
    #     return groupbys, joiners

    def _to_list_noconversion(self, items):
        if isinstance(items, dict):
            items = list(items.values())
        elif isinstance(items, Feature):
            items = [items]
        return items

    def _find_dup_feature(self, feature: Feature, features: dict):
        """
        Searches for a duplicated feature from a list of features.
        :param feature: target feature to search against
        :param features: a list of features
        :return: a duplicated feature, or none if there is no duplication.
        """
        for name, f in features.items():
            if feature._isdup(f):
                return f
        return None

    def _dedup_fast(self, df: DataFrame, features: list):
        """
        Dedups the features already defined in df and feature list. This is the fast version which only checks if the names match.
        :param df: dataframe to attach the features
        :param features: features to be deduped
        :return: deduped features, duplicated features
        """
        deduped_tmp = OrderedDict()
        duplicated = []
        features = self._to_list_noconversion(features)
        logger.info("dedup features: {}".format(features))
        for feature in features:
            if feature.output_alias in deduped_tmp:
                duplicated.append(feature.output_alias)
                logger.warning("Feature {} already defined in features".format(feature.output_alias))
            else:
                deduped_tmp[feature.output_alias] = feature

        deduped = OrderedDict()
        for alias, feature in deduped_tmp.items():
            if feature.output_alias in df.columns:
                duplicated.append(feature.output_alias)
                logger.warning("Feature {} already defined in the dataframe with columns {}.".format(feature.output_alias, df.columns))
            else:
                deduped[feature.output_alias] = feature

        logger.info("deduped features: {}".format(deduped))
        return list(deduped.values()), duplicated

    def _dedup_features(self, df: DataFrame, features: list):
        """
        Please notes: this method needs to be optimized. Current runtime complexity is O(N^2)
        Dedups the features already defined in df and feature list.
        It checks both name and logic of features
        :param df: dataframe to attach the features
        :param features: features to be deduped
        :return: deduped features, duplicated features
        """
        deduped_tmp = {}
        duplicated = []
        features = self._to_list_noconversion(features)
        logger.info("dedup features: {}".format(features))
        for feature in features:
            if self._find_dup_feature(feature, deduped_tmp) is not None:
                duplicated.append(feature.output_alias)
                logger.warning("Feature {} already defined in features".format(feature.output_alias))
                logger.warning("Feature {} already defined in features".format(feature.output_alias))
            else:
                deduped_tmp[feature.output_alias] = feature

        deduped = {}
        for alias, feature in deduped_tmp.items():
            if feature.output_alias in df.columns:
                duplicated.append(feature.output_alias)
                logger.warning("Feature {} already defined in the dataframe with columns {}.".format(feature.output_alias, df.columns))
                logger.warning("Feature {} already defined in the dataframe with columns {}.".format(feature.output_alias, df.columns))
            else:
                deduped[feature.output_alias] = feature

        logger.info("deduped features: {}".format(deduped))
        return deduped.values(), duplicated

    def _is_join_needed(self, from_df: DataFrame, to_df: DataFrame):
        from_cols = from_df.columns
        return not all(self._col_in_df(col, to_df) for col in from_cols)

    def _col_in_df(self, col: str, df: DataFrame):
        lower_df_cols = [c.lower() for c in df.columns]
        return col.lower() in lower_df_cols

    def _validate_col(self, df: DataFrame, *cols):
        """
        Validates if a base_col or join_col actually exists in the dataframe.
        :param df:
        :param cols:
        :return: True - col exists in the df; False - not exists in the df.
        """

        if cols is None or df is None:
            return False, []

        undefined_col = []
        for col in cols:
            if not isinstance(col, Column):
                logger.warning("{} is not a column".format(col))
                return False, []
            col_name = col._jc.toString()
            if str.isnumeric(col_name):
                pass
            else:
                exists = col_name in df.columns
                if not exists:
                    undefined_col.append(col_name)
                    logger.warning("col: {} NOT IN {}".format(col_name, df.columns))

        return len(undefined_col) == 0, undefined_col

    def _register_feature_func(self):
        """
        Registers the function which is annotated with registrar
        Registered functions are stored in registrar.all
        :return: a wrapper function of the annotated function.
        """
        registry = OrderedDict()
        def registrar(func):
            registry[func.__name__] = func
            return func  # normally a decorator returns a wrapped function,
            # but here we return func unmodified, after registering it
        registrar.all = registry
        return registrar

    # def _register_joiner_func(self):
    #     """
    #     Registers the function which is annotated with registrar
    #     Registered functions are stored in registrar.all
    #     :return: a wrapper function of the annotated function.
    #     """
    #     registry = OrderedDict()
    #     def registrar(func):
    #         registry[func.__name__] = func
    #         return func  # normally a decorator returns a wrapped function,
    #         # but here we return func unmodified, after registering it
    #     registrar.all = registry
    #     return registrar

    def get_current_date(self, strfmt: str):
        """
        Gets string of current date using the format specified with strfmt
        :param strfmt:
        :return:
        """
        now = datetime.now()
        return now.strftime(strfmt)

    def int_to_date(self, dt: int, round: str=None) -> datetime.date:
        import calendar
        dt_str = str(dt)
        adjusted_dt = None
        if len(dt_str) == 6:
            strfmt = "%Y%m"
            adjusted_dt = datetime.strptime(str(dt), strfmt).date()
            mrange = calendar.monthrange(adjusted_dt.year, adjusted_dt.month)
            if round is not None:
                if round == "ceiling":
                    adjusted_dt = datetime(year=adjusted_dt.year, month=adjusted_dt.month, day=mrange[1])
                elif round == "floor":
                    adjusted_dt = datetime(year=adjusted_dt.year, month=adjusted_dt.month, day=1)
        else:
            adjusted_dt = datetime.strptime(str(dt), "%Y%m%d").date()
        return adjusted_dt

    def date_to_string(self, dt: datetime.date, fmt="%Y-%m-%d") -> str:
        return dt.strftime(fmt)

    def get_monthid(self, start_dt=datetime.today(), backtrace_days: int=0) -> int:
        import dateutil
        date = backtrace_days
        current_date = start_dt + dateutil.relativedelta.relativedelta(days=-date)
        month_id = current_date.year * 100 + current_date.month
        return month_id

    def subtract_one_month(self, dt: datetime.date):
        dt1 = dt.replace(day=1)
        dt2 = dt1 - timedelta(days=1)
        dt3 = dt2.replace(day=1)
        return dt3

    def subtract_months(self, dt: datetime.date, months: int):
        for i in range(months):
            dt = self.subtract_one_month(dt)
        return dt

    def get_months_range(self, dt: datetime.date, months: int):
        months_range = []
        for i in range(months):
            dt2 = self.subtract_months(dt, i)
            months_range.append(dt2)
        return months_range

class Converter:
    def __init__(self, items, target_type):
        items_t = type(items)
        if items_t is list:
            if len(items) == 0:
                self.list = []
            else:
                self.list = [target_type(i) if type(i) is str else i for i in items]
        elif items_t is dict:
            self.list = [target_type(i) if type(i) is str else i for i in items.values()]
        elif items_t is str:
            self.list = [target_type(i) for i in items.split(",")]
        elif items is None:
            self.list = []
        else:
            self.list = [items]


