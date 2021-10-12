import json
import logging, os, traceback, sys
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column


logger = logging.getLogger(__name__)


class ConfigPlaceHolder(object):
    pass


class ConfigObj:
    # Build config from file, or build/edit config on the fly
    def __init__(self, _conf=None):
        if isinstance(_conf, dict):
            self.configs = _conf
        elif isinstance(_conf, str):
            self.configs = self._build_config_from_json(_conf)
        else:
            self.configs = {}
        self._update_conf_obj(self, _conf)

    def _update_conf_obj(self, obj, mapping):
        # logger.info("updating config obj from dict ...")
        if mapping is not None:
            for k, v in mapping.items():
                if type(v) is not dict:
                    setattr(obj, str(k), v)
                else:
                    conf = ConfigPlaceHolder()
                    conf = self._update_conf_obj(conf, v)
                    setattr(obj, str(k), conf)
        return obj

    def _serialize_value(self, v):
        from framework.feature_factory.data import DataSrc
        if isinstance(v, Column):
            return v._jc.toString()
        elif isinstance(v, DataFrame):
            return "df..."
        elif isinstance(v, DataSrc):
            return "data..."
        else:
            return v

    def _serialize_list(self, lst: list):
        result = []
        for v in lst:
            if isinstance(v, list):
                v_ser = self._serialize_list(v)
                result.append(v_ser)
            elif isinstance(v, dict):
                v_ser = self._serialize(v)
                result.append(v_ser)
            else:
                v_ser = self._serialize_value(v)
                result.append(v_ser)
        return result

    def _serialize(self, config: dict):

        if config is not None:
            new_conf = {}
            for k, v in config.items():
                if isinstance(v, dict):
                    new_conf[k] = self._serialize(v)
                elif isinstance(v, list):
                    new_conf[k] = self._serialize_list(v)
                else:
                    new_conf[k] = self._serialize_value(v)
            return new_conf
        else:
            return None


    def _get_level(self, level: str):
        levels = [l.strip() for l in level.split(".")]
        return self._get_level_list(levels)

    def _get_level_list(self, levels: list):
        try:
            cur = self.configs
            for l in levels:
                if isinstance(cur, dict):
                    if l in cur:
                        cur = cur[l]
                    else:
                        logger.debug("_get_level_list: current level obj does not contain {}.".format(l))
                        return None
                else:
                    logger.debug("_get_level_list: current level obj is not dict.")
                    return None
            return cur
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc(file=sys.stdout)
            return None

    def _parse_config_name(self, config_name: str):
        levels = [l.strip() for l in config_name.split(".")]
        return levels[0:-1], levels[-1]

    def get_config(self, config_name):
        """
        Gets a config object containing the value of config_name if the value is a dict, otherwise gets the value itself.
        The config name can contain the full path of the config with CSV format.
        e.g. get_config("namespace1.namespace2.collector_dim") instead of configs["namespace1"]["namespace2"]["collector_dim"]
        :param config_name: the config to retrieve
        :return: a config object containing the value of the config_name
        """
        conf = self.get_or_else(config_name, {})

        if isinstance(conf, dict):
            return ConfigObj(conf)
        else:
            return conf


    def as_dict(self):
        return self.configs

    def get_or_else(self, config_name, default):
        """
        Gets the value of the config_name. If the config_name does not exist, returns the default.
        The config name can contain the full path of the config with CSV format.
        e.g. get_or_else("namespace1.namespace2.collector_dim", None) instead of configs["namespace1"]["namespace2"]["collector_dim"]
        :param config_name: the config to retrieve
        :param default:
        :return: the value of the config_name
        """
        dir, k = self._parse_config_name(config_name)
        level_obj = self._get_level_list(dir)
        if k == '':
            return level_obj
        return level_obj[k] if level_obj is not None and k in level_obj else default

    def contains(self, config_name):
        """
        Checks if the config object contains the config_name.
        The config name can contain the full path of the config with CSV format.
        e.g. contains("namespace1.namespace2.collector_dim")
        :param config_name:
        :return:
        """
        conf = self._get_level(config_name)
        return conf is not None

    def isempty(self):
        """
        Checks if the config object is empty or not.
        :return:
        """
        return len(self.configs) == 0

    def merge(self, conf):
        """
        conf_obj will be merged into this config obj. The current config will be updated by the config_obj.
        :param conf_obj:
        :return:
        """
        conf = conf.configs if isinstance(conf, ConfigObj) else conf
        keys_1 = set(self.configs.keys())
        keys_2 = set(conf.keys())
        intersection = keys_1 & keys_2
        if len(intersection) > 0:
            logger.warning("Merge failed because of conflicts in keys {}".format(intersection))
        else:
            self.configs = {**self.configs, **conf}
            self._update_conf_obj(self, self.configs)

    def add(self, config_name: str, _config):
        """
        adds a new config at a specified location.
        e.g. conf.add("key", "value", "under.this.level")
        :param config_name:
        :param _config:
        :param level:
        :return:
        """
        level, k = self._parse_config_name(config_name)
        obj = self.configs
        for l in level:
            if l in obj:
                obj = obj[l]
                if not isinstance(obj, dict):
                    logger.debug("{} is not a dictionary. cannot add config to it.".format(l))
                    return self
            else:
                obj[l] = {}
                logger.debug("key {} did not existed. A new dict is created for the key.".format(l))
                obj = obj[l]

        if isinstance(obj, dict):
            obj[k] = _config
            self._update_conf_obj(self, self.configs)
        else:
            logger.info("{} is not a dictionary. cannot add config to it.".format(level))

        return self

    def drop(self, config_name: str):
        """
        Removes the config (can be a dict or a value) from config object
        The config name can contain the full path of the config with CSV format.
        e.g. drop("namespace1.namespace2.collector_dim") will remove configs["namespace1"]["namespace2"]["collector_dim"]
        :param config_name:
        :return:
        """
        level, k = self._parse_config_name(config_name)
        level_obj = self._get_level_list(level)
        if isinstance(level_obj, dict):
            del level_obj[k]
            logging.info("{} deleted from {}".format(k, level))
        else:
            logging.info("{} does not contain {}".format(level, k))

        return self

    def export(self, path: str, dbutils):
        """
        Saves the config to a file on dbfs (dbfs:/) or local file system (file:///).
        :param path:
        :param dbutils:
        :return:
        """
        json_str = json.dumps(self.configs)
        dbutils.fs.put(path, json_str)

    def print(self, config_name: str=''):
        """
        Pretty print the json obj retrieved using given config_name.
        By default, prints out the entire config.
        :param config_name:
        :return:
        """
        cur_config = self.get_or_else(config_name, "")
        new_config = self._serialize(cur_config)

        print(json.dumps(new_config, indent=4, sort_keys=True))

    def _build_config_from_json(self, path:str, dbutils):
        config_holder = {}
        filename = os.path.basename(path)
        local_path = os.path.join("file:///tmp", filename)
        dbutils.fs.cp(path, local_path)
        with open(os.path.join("/tmp", filename), "r") as f:
            config_holder = json.load(f)
        return config_holder
