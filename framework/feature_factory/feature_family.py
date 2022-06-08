import inspect
from pyspark.sql.functions import col, lit, pandas_udf, PandasUDFType
from framework.feature_factory.feature import Feature, FeatureSet
from collections import OrderedDict
from framework.configobj import ConfigObj
from datetime import datetime
from pyspark.sql.types import DoubleType, ArrayType, IntegerType
import pandas as pd
from scipy.optimize import curve_fit


class FeatureFamily:
    """
    Base class to derive concrete feature families, e.g. issuance, sales.
    """
    def __init__(self, _config: ConfigObj):
        self._features = OrderedDict()
        self._multipliable_features = OrderedDict()
        self._base_features = OrderedDict()
        self._groupy_cols = []
        self.config = _config
        # self._populate_joiners()
        self._build_all()

    # before appending any features make sure it doesn't already exist
    # We need a get all features function for feature Family

    def get(self):
        """
        Gets features built so far and resets the feature collection.
        :return:
        """
        new_features = OrderedDict(self._features)
        self._reset_features()
        # for n, f in new_features.items():
        #     f._populate_joiner_df(self.config)
        return new_features

    def _create_feature(self, frame):
        """
        Creates a feature based on the arguments passed in from frame.
        :param frame:
        :return:
        """
        args, _, _, values = inspect.getargvalues(frame)
        dict = {i: values[i] for i in args}
        _name = dict["_name"]

        exist_feat = self._get_feature(_name)
        if exist_feat:
            self._features[_name] = exist_feat
        else:
            del dict["self"]
            if "_col_alias" in dict:
                del dict["_col_alias"]
            base_col = dict["_base_col"]
            dict["_base_col"] = col(base_col) if type(base_col) is str else lit(base_col)
            input_config = ConfigObj(dict)
            self._features[_name] = Feature(_name=input_config.get_or_else("_name", ""),
                                            _base_col=input_config.get_or_else("_base_col", None),
                                            _filter=input_config.get_or_else("_filter", []),
                                            _negative_value=input_config.get_or_else("_negative_value", 0),
                                            _agg_func=input_config.get_or_else("_agg_func", None),
                                            _agg_alias=input_config.get_or_else("_agg_alias", None),
                                            _kind=input_config.get_or_else("_kind", "multipliable"))

        return self._features[_name]


    def _get_feature(self, _name: str):
        feature = None
        if _name in self._multipliable_features:
            feature = self._multipliable_features[_name]
        elif _name in self._base_features:
            feature = self._base_features[_name]
        return feature

    def _feature_exists(self, _features: OrderedDict, _name: str):
        return _name in _features


    def _reset_features(self):
        self._features = OrderedDict()

    # def _populate_joiners(self):
    #     if hasattr(self, "_joiner_func"):
    #         for join_key, func in self._joiner_func.all.items():
    #             func(self)

    def get_all(self):
        self._features = OrderedDict(self._multipliable_features)
        multipliable = self.get()
        self._features = OrderedDict(self._base_features)
        base_features = self.get()
        return FeatureSet(multipliable), FeatureSet(base_features)

    @pandas_udf(ArrayType(DoubleType()))
    def trend_fit(v):
        """ fit a simple curve on yValues. y = Ax + B
        row -- given row
        featureName -- prefix of repating features f_1, f_2, .., f_n ---- TOTAL_MILES_1
        featureName is someting you calculated montly counts sums avgs
        fromIndex, toIndex. to select f_x falues.

        yValues -- will be derived from row as a list. Ex. [2,7,12]
        corresponding xValues  [1,2,3] created internally

        import the lib: from scipy.optimize import curve_fit
        https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.curve_fit.html#scipy.optimize.curve_fit
        return -- A, slope
        """
        res = []

        for yvals in v:
            xValues = [i for i in range(len(yvals), 0, -1)]
            yValues = [float(y) for y in yvals]

            print('yValues: ', yValues)
            print('xValues: ', xValues)
            f = lambda x, A, B: A * x + B  # you are fitting a curve y = Ax + B
            A, B = curve_fit(f, xValues, yValues)[0]  # see scipy lib
            print('y=Ax +B --> A: ', -A, '  B: ', B)
            res.append([-A, B])
        return pd.Series(res)

    def _get_all_multipliable(self):
        self._features = OrderedDict(self._multipliable_features)
        return self.get()

    def get_all_base_features(self):
        self._features = OrderedDict(self._base_features)
        return FeatureSet(self.get())

    def list_features(self):
        all_features = [*(self._base_features.keys()), *(self._multipliable_features.keys())]
        return all_features

    def _build_all_multipliable(self):
        if hasattr(self, "_multipliable_feat_func"):
            for fname, func in self._multipliable_feat_func.all.items():
                self = func(self)
            self._multipliable_features = self.get()
            for n, f in self._multipliable_features.items():
                f.kind = "multipliable"

    def _build_all_base(self):
        if hasattr(self, "_base_feat_func"):
            for fname, func in self._base_feat_func.all.items():
                self = func(self)
            new_features = self.get()
            self._base_features = OrderedDict()
            for n, f in new_features.items():
                if not self._feature_exists(self._multipliable_features, n):
                    f.kind = "base"
                    self._base_features[n] = f

    def _build_all(self):
        self._build_all_multipliable()
        self._build_all_base()

