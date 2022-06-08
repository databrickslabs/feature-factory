from framework.feature_factory.feature_family import FeatureFamily
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, array
from framework.feature_factory.feature import Feature, Multiplier, FeatureSet
from framework.feature_factory.dtm import DateTimeManager
from framework.configobj import ConfigObj
from collections import OrderedDict


class TrendsCommon(FeatureFamily):
    def __init__(self, _featureSet_to_trend, _trend_period_ranges,
                 _dtm: DateTimeManager, _time_helprs_config=ConfigObj()):
        """

        :param _featureSet_to_trend:
        :param _dtm: This dtm needs to contain a single type of period such as only w or only m
        passing in mixed ranges is not supported for trending yet
        :param _time_helprs_config:
        """
        # FeatureFamily.__init__(self, config)
        self.time_helpers_config = _time_helprs_config
        self.dtm = _dtm
        # self.months = _months
        self.featureSet_to_trend = _featureSet_to_trend
        self.trend_period_ranges = _trend_period_ranges

        FeatureFamily.__init__(self, _time_helprs_config)

    def transfer_features(self, cls):
        for fn, func in self._feat_func.all.items():
            setattr(cls, fn, func)

    def get_all(self):
        """

        :return:
        """

        intermediate_feature_sets = []
        final_features = FeatureSet()

        self.dtm._append_trend_ranges(self.trend_period_ranges)
        self.time_helpers_config = self.dtm.get_config().get_config("time_helpers")
        trend_ranges = self.time_helpers_config.get_config("trend_filters")

        for trend_period, trend_filter in trend_ranges.as_dict().items():
            period_max = max([(int(x[:-1])) for x in trend_ranges.get_or_else("{}.ranges".format(trend_period),
                                                                              "{} does not exist in trend_ranges"
                                                                              .format(trend_period))])
            # period_max = max(details['periods'])
            for in_feature_name, in_feature in self.featureSet_to_trend.features.items():
                # Add a filter here to allow for more types of trendings
                # Add a filter for date_col and then derive part_col filter from it
                times_multiplier = Multiplier._create_from_daterange(self.dtm,
                                                                     self.time_helpers_config,
                                                                     period_ranges="trend_filters.{}.ranges".format(trend_period))
                fset = FeatureSet(OrderedDict({in_feature.name: in_feature}))
                source_trend_features = fset.multiply(times_multiplier, "TREND", is_temporary=True)
                names = list(source_trend_features.features.keys())
                slope_feature = Feature(_name="{}_trend".format(in_feature_name),
                                        _base_col=FeatureFamily.trend_fit(array(names)),
                                        _is_temporary=True
                                        )
                source_trend_features.features[slope_feature.name] = slope_feature
                intermediate_feature_sets.append(source_trend_features)

            for feature in [feature for feature_set in intermediate_feature_sets
                            for (fn, feature) in feature_set.features.items() if "_trend" in fn]:
                final_features.add_feature(Feature(_name="STORE_" + feature.name.upper()
                                                         + "_SLOPE_{}{}".format(period_max,
                                                                                trend_period.upper()),
                                                   _base_col=feature.base_col[0]))
                final_features.add_feature(Feature(_name="STORE_" + feature.name.upper()
                                                         + "_INTERCEPT_{}{}".format(period_max,
                                                                                    trend_period.upper()),
                                                   _base_col=feature.base_col[1]))

        # Get a single feature set to return
        for feature_set in intermediate_feature_sets:
            for fn, feature in feature_set.features.items():
                final_features.add_feature(feature)

        return final_features

