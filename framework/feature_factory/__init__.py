from pyspark.sql.functions import col, lit, when, struct
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from framework.feature_factory.feature import Feature, FeatureSet, Multiplier
from framework.configobj import ConfigObj
from framework.feature_factory.helpers import Helpers
import re
import logging
import datetime
import inspect
from collections import OrderedDict

logger = logging.getLogger(__name__)

class Feature_Factory():

    def __init__(self):
        self.helpers = Helpers()

    def append_features(self, df: DataFrame, groupBy_cols, feature_sets: [FeatureSet], withTrendsForFeatures: [FeatureSet] = None):
        """
        Appends features to incoming df. The features columns and groupby cols will be deduped and validated.
        If there's a group by, the groupby cols will be applied before appending features.
        If there's not a group by and no agg features then the features will be appended to df.
        :param df:
        :param groupBy_cols:
        :param feature_sets: input of FeatureSet
        :return:
        """
        # If groupBy Column is past in as something other than list, convert to list
        # Validation - If features, passed in is dict, convert to list of vals, etc.
        # groupBy_cols = self.helpers._to_list(groupBy_cols)
        # groupBy_cols, groupBy_joiners = self.helpers._extract_groupby_joiner(groupBy_cols)
        groupBy_cols = [gc.assembled_column if isinstance(gc, Feature) else gc for gc in groupBy_cols]
        features, dups = self.helpers._dedup_fast(df, [feature for feature_set in feature_sets for feature in feature_set.features.values()])
        # df = self.helpers._resolve_feature_joiners(df, features, groupBy_joiners).repartition(*groupBy_cols)
        df = df.repartition(*groupBy_cols)

        # feature_cols = []
        agg_cols = []
        non_agg_cols = {}
        features_to_drop = []
        # base_cols = [f.base_col for f in features]

        # column validation
        # valid_result, undef_cols = self.helpers.validate_col(df, *base_cols)
        # assert valid_result, "base cols {} are not defined in df columns {}".format(undef_cols, df.columns)

        # valid_result, undef_cols = self.helpers._validate_col(df, *groupBy_cols)
        # assert valid_result, "groupby cols {} are not defined in df columns {}".format(undef_cols, df.columns)
        for feature in features:
            assert True if ((len(feature.aggs) > 0) and (len(
                groupBy_cols) > 0) or feature.agg_func is None) else False, "{} has either aggs or groupBys " \
                                               "but not both, ensure both are present".format(feature.name)
            # feature_cols.append(feature.assembled_column)
            # feature_cols.append(F.col(feature.output_alias))
            agg_cols += [agg_col for agg_col in feature.aggs]
            if feature.agg_func is None:
                non_agg_cols[feature.output_alias] = feature.assembled_column
            else:
                df = df.withColumn(feature.output_alias, feature.assembled_column)

            if feature.is_temporary:
                features_to_drop.append(feature.name)

        if len(groupBy_cols) > 0:
            df = df.groupBy(*groupBy_cols)\
                .agg(*agg_cols)
        for fn, col in non_agg_cols.items():
            df = df.withColumn(fn, col)

        final_df = df.drop(*features_to_drop)
        # else:
        #     new_df = df.select(*df.columns + feature_cols)
        return final_df


