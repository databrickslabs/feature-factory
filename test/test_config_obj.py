import unittest
from framework.configobj import ConfigObj
import json

class TestConfigObj(unittest.TestCase):
    def setUp(self):
        pass

    def test_add_config(self):
        config_obj = ConfigObj({"a": 1, "b":{"c": [1,2,3,4,5]}})
        config_obj.add("b.c2", 2)
        config_obj.add("e1.e2.e3", 123)
        print(config_obj.configs)
        assert config_obj.b.c2 == 2 and config_obj.b.c == [1,2,3,4,5], "should add config to config obj."
        assert config_obj.e1.e2.e3 == 123, "should be able to chain add."
        assert config_obj.get_or_else("e1.e2.e3", None) == 123, "should be able to get config value."
        assert config_obj.get_or_else("e1.e22", "default") == "default", "default value if key does not exist."

    def test_get_config(self):
        config_obj = ConfigObj({"a": 1, "b":{"c": [1,2,3,4,5]}})
        conf = config_obj.get_config("b")
        assert conf.c == [1,2,3,4,5], "get_config should return a config obj."
        conf = config_obj.get_config(("b.c"))
        assert conf == [1,2,3,4,5], "get_config should return a value."
        conf = config_obj.get_config("c")
        print("conf: {}".format(conf))

    def test_merge_config(self):
        config_obj = ConfigObj({"a": 1, "b":{"c": [1,2,3,4,5]}})
        config_obj.merge({"e1": {"e2": 12}})
        assert config_obj.e1.e2 == 12, "error merging two configs"
        config_obj.print()

        config_obj = ConfigObj({"a": 1, "b":{"c": [1,2,3,4,5]}})
        config_obj.merge(ConfigObj({"e1": {"e2": 12}}))
        assert config_obj.e1.e2 == 12, "error merging two configs"

        config_obj = ConfigObj({"a": 1, "b":{"c": [1,2,3,4,5]}})
        config_obj.merge(ConfigObj({"a": {"e2": 12}}))
        assert len(config_obj.configs) == 2, "merge should fail because of conflicts in keys."

    def test_contains_config(self):
        config_obj = ConfigObj({"a": 1, "b":{"c": [1,2,3,4,5]}})
        assert config_obj.contains("b.c"), "config should contains b.c"
        assert not config_obj.contains("b.c.e"), "config should not contains b.c.e"
        assert config_obj.contains("a"), "config should contains a"
        assert not config_obj.contains("d"), "config should not contains d"

    def test_drop_config(self):
        config_obj = ConfigObj({"a": 1, "b":{"c": [1,2,3,4,5]}})
        config_obj.drop("b.c").print()
        assert len(config_obj.get_or_else("b", None)) == 0, "b.c should be removed"

    def test_add_config(self):
        config_obj = ConfigObj(dict())
        config_obj.add("namespace1.namespace2", {"setting1":"value1", "setting2": "value2"})
        assert config_obj.get_or_else("namespace1.namespace2.setting1", "") == "value1"

    def test_key_as_int(self):
        channel_config = ConfigObj()
        channel_config.add("promo_pre_periods", {
            # list of promo length / pre-period length pairs in days
            7: 28,
            14: 28,
            21: 56
        })
        assert channel_config.as_dict()["promo_pre_periods"][7] == 28

    def tearDown(self):
        pass