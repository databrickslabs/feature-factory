from pyspark.sql.functions import col, lit, when
from pyspark.sql.column import Column
from functools import reduce
from collections import OrderedDict
from framework.configobj import ConfigObj
from framework.feature_factory.dtm import DateTimeManager
import datetime


class Feature:
    """
    Feature class to be appended by Feature Factory.
    """
    def __init__(self,
                 _name: str,
                 _base_col: Column,
                 _filter=[],
                 _negative_value=0,
                 _agg_func=None,
                 _agg_alias:str=None,
                 _kind="multipliable",
                 _is_temporary=False):
        """

        :param _name: name of the feature
        :param _base_col: the column(s) the feature is derived from when the filter is true or there is no filter.
            Note base_col can be multiple cols, e.g. base_col = F.concat(F.col("POS_TRANS_ID"), F.col("TRANS_DATE"), F.col("STORE_ID"), F.col("POS_TILL_NUM")
        :param _filter: the condition to choose base_col or nagative_value
        :param _negative_value: the value the feature is derived from when the filter condition is false
        :param _agg_func: the aggregation functions for computing the feature value from base_col or negative_value
        :param _agg_alias: alias name
        """
        self.name = _name
        if isinstance(_base_col, Feature):
            self.base_col = _base_col.assembled_column
        else:
            self.base_col = _base_col if type(_base_col) is Column else col(_base_col)
        self.filter = _filter if type(_filter) is list else [_filter]
        self.negative_value = _negative_value if _negative_value != "" else None
        self.output_alias = _name if _agg_alias is None else _agg_alias
        self.agg_func = _agg_func
        self.aggs = []
        self.assembled_column = None
        self._assemble_column()
        self.kind = _kind
        self.is_temporary = _is_temporary

    def _clone(self, _alias: str=None):
        alias = _alias if _alias is not None else self.output_alias
        return Feature(alias, self.base_col, self.filter, self.negative_value, self.agg_func, alias, self.kind)

    def _assemble_column(self):
        if (self.base_col is not None) and (len(self.filter) > 0) and (self.agg_func is not None):
            self.assembled_column = when(self._assemble_filter(), self.base_col).otherwise(self.negative_value).alias(self.output_alias)
            self._assemble_aggs()
        elif (self.base_col is not None) and (len(self.filter) == 0) and (self.agg_func is not None):
            self.assembled_column = self.base_col.alias(self.output_alias)
            self._assemble_aggs()
        elif (self.base_col is not None) and (len(self.filter) > 0) and (self.agg_func is None):
            self.assembled_column = when(self._assemble_filter(), self.base_col).otherwise(self.negative_value).alias(self.output_alias)
        else:
            self.assembled_column = self.base_col.alias(self.output_alias)

    def _assemble_aggs(self):
        self.aggs.append(self.agg_func(self.output_alias).alias(self.output_alias))

    def _assemble_filter(self):
        if len(self.filter) == 1:
            return self.filter[0]
        else:
            final_filter = reduce((lambda x, y: x & y), self.filter)
            return final_filter

    def _equals(self, that):
        this_expr = self.assembled_column._jc.toString()
        that_expr = that.assembled_column._jc.toString()
        return this_expr == that_expr

    def _isdup(self, that):
        # one dataframe cannot have two columns with the same name
        if self.output_alias == that.output_alias:
            return True
        # when the alias are not the same, check the dup of logic
        this_copy = self._clone("colname")
        that_copy = that._clone("colname")
        return this_copy._equals(that_copy)


class FeatureSet:

    def __init__(self, _features: OrderedDict=OrderedDict(), _name_prefix:str = ""):
        from framework.feature_factory.helpers import Helpers
        self.helpers = Helpers()
        _features = _features.features if isinstance(_features, FeatureSet) else _features
        self.features = OrderedDict()
        for fn, f in _features.items():
            if f.name.startswith(_name_prefix):
                self.features[fn] = f
            else:
                new_name = self.helpers._clean_alias("{}{}".format(_name_prefix, f.name))
                new_f = f._clone(new_name)
                self.features[new_name] = new_f

        self.columns = self.features.keys()


    # Allow addition of individual feature in a certain position
    def add_feature(self, _feature: Feature, pos: int=0):
        name = _feature.name
        self.features[name] = _feature

    def remove_feature(self, _name: str):
        del self.features[_name]

    def extract_multipliable_name(self, _name_prefix: str, _feature_name: str):
        if _feature_name.startswith(_name_prefix):
            return '_'.join(_feature_name.split("_")[1:])
        else:
            return _feature_name

    def multiply(self, _multiplier, _name_prefix: str, is_temporary=False):
        _name_prefix = _name_prefix.upper()
        nrow = len(_multiplier.filters)

        results = OrderedDict()
        for base_feature in self.features.values():
            if base_feature.kind == "multipliable":
                for r in range(nrow):
                    for c in range(len(_multiplier.filters[r])):
                        current_name = _multiplier.filter_names[r][c] if _multiplier.filter_names is not None else _multiplier.filter_vals[r][c]
                        multipliable_name = self.extract_multipliable_name(_name_prefix, base_feature.name)
                        feature_name = self.helpers._clean_alias(
                            "{}_{}_{}".format(_name_prefix, current_name.upper(), multipliable_name.upper()))
                        feature_filter = [*base_feature.filter, *_multiplier.filters[r][c]]
                        results[feature_name] = Feature(_name=feature_name,
                                                        _base_col=base_feature.base_col,
                                                        _filter=feature_filter,
                                                        _negative_value=base_feature.negative_value,
                                                        _agg_func=base_feature.agg_func,
                                                        _is_temporary=is_temporary
                                                        )
        return FeatureSet(results)


class Multiplier:
    def __init__(self, _filter_cols: list, _filter_vals: list, _filters: list, _filter_names: list=None):
        self.filter_cols = _filter_cols
        # self.filter_type = _filter_type
        self.filter_vals = _filter_vals
        self.filters = _filters
        self.filter_names = _filter_names

    @classmethod
    def _create_from_cats(cls, _filter_cols: list, _filter_vals: list, _filter_names: list=None):
        nrow = len(_filter_vals)
        filters = []
        filter_names = []
        for r in range(nrow):
            filters.append([])
            filter_names.append([])
            for c in range(len(_filter_vals[r])):
                filters[r].append([col(_filter_cols[r]) == _filter_vals[r][c]])
                filter_names[r].append("{}-{}".format(_filter_cols[r], _filter_vals[r][c]))
        return Multiplier(_filter_cols, _filter_vals, filters, filter_names)

    @classmethod
    def _create_from_daterange(cls, dtm: DateTimeManager,
                               time_helpers: ConfigObj,
                               period_ranges="date_filters.ranges",):
        """
               :param time_helpers: {
                   snapshot_date: "20190225",
                   snapshot_type: "DAILY",
                   partition_col: "p_yyyymm",
                   date_ranges: {
                       "1m": {"start": <val>, "end": <val>, "3m": {"start": <val>, "end": <val>}
                       }
               }
               :return:
               """
        filters = []
        filter_vals = []
        date_ranges = time_helpers.get_config(period_ranges).as_dict()
        partition_col = time_helpers.get_or_else("partition_col", "")
        filter_names = list(date_ranges.keys())
        filter_cols = [partition_col]

        if time_helpers.get_or_else("snapshot_type", "MONTH") == "DAILY":
            date_col = time_helpers.get_or_else("date_col",
                                                "No date col specified in the time_helpers config."
                                                "Daily snapshots require a date_col to be defined")
            filter_cols = [partition_col, date_col]
            for date_range in date_ranges.keys():
                d_filter = dtm.scoped_time_filter(range_dict=date_ranges[date_range])
                filters.append([d_filter])
                filter_vals.append(date_ranges[date_range])

        else:
            for date_range in date_ranges.keys():
                p_filter = dtm.scoped_partition_filter(range_dict=date_ranges[date_range])
                filters.append([p_filter])
                filter_vals.append(date_ranges[date_range])

        return Multiplier(filter_cols, [filter_vals], [filters], [filter_names])

    @classmethod
    def _create_from_months(cls, snapshot_dt: datetime.date, trend_time_col, month_durs: int):
        from framework.feature_factory.helpers import Helpers
        helpers = Helpers()

        filters = []
        filter_vals = []
        filter_names = ["{}m".format(i+1) for i in range(month_durs)]
        filter_cols = [trend_time_col]
        months_range = helpers.get_months_range(snapshot_dt, month_durs)
        for m in reversed(months_range):
            target_month_id = helpers.get_monthid(m)
            t_filter = col(trend_time_col) == target_month_id
            filters.append([t_filter])
            filter_vals.append(target_month_id)

        return Multiplier(filter_cols, [filter_vals], [filters], [filter_names])
