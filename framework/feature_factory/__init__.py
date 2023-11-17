from pyspark.sql.functions import col, lit, when, struct
from pyspark.sql.column import Column
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.dataframe import DataFrame
from framework.feature_factory.feature import Feature, FeatureSet, Multiplier
from framework.configobj import ConfigObj
from framework.feature_factory.helpers import Helpers
from framework.feature_factory.agg_granularity import AggregationGranularity
from framework.feature_factory.llm_tools import LLMFeature, LLMUtils
import re
import logging
import datetime
import inspect
from collections import OrderedDict
from typing import List
from enum import Enum

logger = logging.getLogger(__name__)

class Feature_Factory():

    def __init__(self):
        self.helpers = Helpers()

    def append_features(self, df: DataFrame, groupBy_cols, feature_sets: List[FeatureSet], withTrendsForFeatures: List[FeatureSet] = None, granularityEnum: Enum = None):
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
        groupBy_cols = [gc.assembled_column if isinstance(gc, Feature) else gc for gc in groupBy_cols]
        features, dups = self.helpers._dedup_fast(df, [feature for feature_set in feature_sets for feature in feature_set.features.values()])
        df = df.repartition(*groupBy_cols)

        # feature_cols = []
        agg_cols = []
        non_agg_cols = {}
        features_to_drop = []
        
        granularity_validator = AggregationGranularity(granularityEnum) if granularityEnum else None
        for feature in features:
            assert True if ((len(feature.aggs) > 0) and (len(
                groupBy_cols) > 0) or feature.agg_func is None) else False, "{} has either aggs or groupBys " \
                                               "but not both, ensure both are present".format(feature.name)
            if granularity_validator:
                granularity_validator.validate(feature, groupBy_cols)
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

        return final_df

    def append_catalog(self, df: DataFrame, groupBy_cols, catalog_cls, feature_names = [], withTrendsForFeatures: List[FeatureSet] = None, granularityEnum: Enum = None):
        """
        Appends features to incoming df. The features columns and groupby cols will be deduped and validated.
        If there's a group by, the groupby cols will be applied before appending features.
        If there's not a group by and no agg features then the features will be appended to df.
        :param df:
        :param groupBy_cols:
        :param feature_sets: input of FeatureSet
        :return:
        """
        # dct = self._get_all_features(catalog_cls)
        dct = catalog_cls.get_all_features()
        fs = FeatureSet(dct)
        return self.append_features(df, groupBy_cols, [fs], withTrendsForFeatures, granularityEnum)

    def assemble_llm_feature(self, spark: SparkSession, srcDirectory: str, llmFeature: LLMFeature, partitionNum: int):
        """
        Creates a dataframe which contains only one column named as llmFeature.name.
        The method will distribute the files under srcDirectory to the partitions determined by the partitionNum.
        Each file will be parsed and chunked using the reader and splitter in the llmFeature object.
        :param spark: a spark session instance
        :param srcDirectory: the directory containing documents to parse
        :llmFeature: the LLM feature instance 
        :partitionNum: the number of partitions the src documents will be distributed onto.
        """
        all_files = self.helpers.list_files_recursively(srcDirectory)
        src_rdd = spark.sparkContext.parallelize(all_files, partitionNum)

        rdd = src_rdd.mapPartitions(lambda partition_data: LLMUtils.process_docs(partitionData=partition_data, llmFeat=llmFeature)).repartition(partitionNum)
        rdd.cache()
        df = rdd.toDF([llmFeature.name])
        return df.select(F.explode(llmFeature.name).alias(llmFeature.name))