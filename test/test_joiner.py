import unittest
from framework.feature_factory import Helpers
from channelDemoStore import Store
from test.local_spark_singleton import SparkSingleton


class TestJoiner(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSingleton.get_instance()
        self.helpers = Helpers()
        self.join_key = "joiners.issuance.partner_offer_dim"
        self.source_name = "partner_offer_dim"

    def test_populate_joiners(self):
        partner = Store(_snapshot_date="2019-01-30")
        joiners_config = partner.config.get_or_else("joiners.sales", {})
        assert len(joiners_config) == 3

    def test_joiner_count(self):
        partner = Store(_snapshot_date="2019-01-30")
        joiners_config = partner.config.get_or_else("joiners.sales", {})
        features, _ = partner.sales.get_all()
        f = features.features["net_sales"]
        assert len(f.joiners) == 0

    def tearDown(self):
        self.spark.stop()