import unittest
from framework.feature_factory.feature import Feature, FeatureSet, CompositeFeature
from framework.feature_factory.feature_dict import ImmutableDictBase
from framework.feature_factory import Feature_Factory
from framework.feature_factory.helpers import Helpers
import pyspark.sql.functions as f
import json
from pyspark.sql.types import StructType
from test.local_spark_singleton import SparkSingleton
from framework.feature_factory.catalog import CatalogBase
from enum import IntEnum

spark = SparkSingleton.get_instance()
class CommonCatalog(CatalogBase):
    total_sales = Feature.create(
        base_col=f.col("ss_net_paid").cast("float"),
        agg_func=f.sum
    )

    total_quants = Feature.create(base_col=f.col("ss_quantity"),
                                           agg_func=f.sum)

class Granularity(IntEnum):
            PRODUCT_ID = 1,
            PRODUCT_DIVISION = 2,
            COUNTRY = 3

class SalesCatalog(CommonCatalog):
    _valid_sales_filter = f.col("ss_net_paid") > 0

    total_sales = Feature.create(
        base_col=CommonCatalog.total_sales,
        filter=_valid_sales_filter,
        agg_func=f.sum,
        agg_granularity=Granularity.PRODUCT_DIVISION
    )


def generate_sales_catalog(CommonCatalog):
    class SalesCatalog(CommonCatalog):
        _valid_sales_filter = f.col("ss_net_paid") > 0

        total_sales = Feature.create(
            base_col=CommonCatalog.total_sales,
            filter=_valid_sales_filter,
            agg_func=f.sum
        )
    return SalesCatalog

class TestSalesCatalog(unittest.TestCase):
    def setUp(self):
        with open("test/data/sales_store_schema.json") as fp:
            sales_schema = StructType.fromJson(json.load(fp))
            df = SparkSingleton.get_instance().read.csv("test/data/sales_store_tpcds.csv", schema=sales_schema, header=True)
            self.sales_df = df.withColumn("PRODUCT_ID", f.lit("product"))\
                .withColumn("PRODUCT_DIVISION", f.lit("division"))\
                .withColumn("COUNTRY", f.lit("country"))
    
    def test_append_catalog(self):
        customer_id = f.col("ss_customer_sk").alias("customer_id")
        ff = Feature_Factory()
        df = ff.append_catalog(self.sales_df, [customer_id], SalesCatalog)
        assert df.count() > 0
        assert "total_sales" in df.columns and "total_quants" in df.columns

    def test_common_catalog(self):
        customer_id = f.col("ss_customer_sk").alias("customer_id")
        ff = Feature_Factory()
        salesCatalogClass = generate_sales_catalog(CommonCatalog=CommonCatalog)
        df = ff.append_catalog(self.sales_df, [customer_id], salesCatalogClass)
        assert df.count() > 0
        assert "total_sales" in df.columns and "total_quants" in df.columns

    def test_granularity(self):
        customer_id = f.col("ss_customer_sk").alias("customer_id")
        ff = Feature_Factory()
        df = ff.append_catalog(self.sales_df, [customer_id, "PRODUCT_ID"], SalesCatalog, granularityEnum=Granularity)
        assert df.count() > 0
