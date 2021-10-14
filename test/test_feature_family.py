import unittest
from framework.feature_factory.feature import Feature
from framework.feature_factory.feature_family import FeatureFamily
from framework.configobj import ConfigObj
import pyspark.sql.functions as F
from channelDemoStore import Store
from channelDemoWeb import Web
from channelDemoCatalog import Catalog
from test.local_spark_singleton import SparkSingleton

class TestFeatureFamily(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSingleton.get_instance()
        data_base_dir = "test/data"
        sales_df = self.spark.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_store_sales_enhanced.csv", inferSchema=True,
                                                               header=True)
        sales_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_store_sales_enhanced")
        store_df = self.spark.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_store.csv", inferSchema=True,
                                                               header=True)
        store_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_store")

        web_sales_df = self.spark.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_web_sales_enhanced.csv", inferSchema=True,
                                       header=True)
        web_sales_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_web_sales_enhanced")
        item_df = self.spark.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_item.csv", inferSchema=True,
                                       header=True)
        item_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_item")
        inventory_df = self.spark.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_inventory.csv", inferSchema=True,
                                      header=True)
        inventory_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_inventory")
        date_df = self.spark.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_date_dim.csv", inferSchema=True,
                                      header=True)
        date_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_date_dim")

        cat_sales_df = self.spark.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_catalog_sales_enhanced.csv",
                                           inferSchema=True,
                                           header=True)
        cat_sales_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_catalog_sales_enhanced")

    def test_store_sales(self):
        store = Store(_snapshot_date="2002-01-31")
        print(store.config.configs)
        assert len(store.sources) == 3
        multipliable, base = store.Sales().get_all()
        assert len(multipliable.features) > 0
        assert store.get_data("sources.store")

    def test_web_sales(self):
        web = Web(_snapshot_date="2002-01-31")
        print(web.config.configs)
        assert len(web.sources) == 1
        multipliable, base = web.Sales().get_all()
        assert len(multipliable.features) > 0

    def test_catalog_sales(self):
        catalog = Catalog(_snapshot_date="2002-01-31")
        print(catalog.config.configs)
        assert len(catalog.sources) > 0
        multipliable, base = catalog.Sales().get_all()
        assert len(multipliable.features) > 0

    def test_trend(self):
        store = Store(_snapshot_date="2002-01-31")
        mult_features, _ = store.Sales().get_all()
        trends = store.Trends(mult_features, [["1w", "12w"], ["1m", "12m"]]).get_all()
        assert len(trends.features) > 0

    def tearDown(self):
        self.spark.stop()