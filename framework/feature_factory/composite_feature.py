from pyspark.sql.functions import col, lit, when
from pyspark.sql.column import Column
from functools import reduce
from collections import OrderedDict
from framework.configobj import ConfigObj
from framework.feature_factory.dtm import DateTimeManager
from framework.feature_factory.feature import FeatureSet


class CompositeFeature:
    """
    CompositeFeature class.
    With composite feature C = Feature A / Feature B
    C.multiply([1M, 3M, 6M]) = A.multiply([1M, 3M, 6M]) / B.multiply([1M, 3M, 6M])
                             = [A_1M/B_1M, A_3M/B_3M, A_6M/B_6M]
    """
    def __init__(self,
                name,
                operand1,
                op,
                operand2):
        """
        :param operand1: a Feature object as left operand
        :param op: a String as operator
        :param operand2: a Feature object as right operand
        """
        self.name = name
        self.operand1 = operand1
        self.operand2 = operand2
        self.op = op
    
    def _internal_ops(self, fs1, fs2):
        if self.op == "/":
            return fs1.divide(fs2, self.name)
        else:
            raise AttributeError("Composite feature only supports / + -")

    
    def multiply(self, multiplier, name_prefix: str="", is_temporary=False):
        fs1 = FeatureSet()
        fs1.add_feature(self.operand1)
        fs2 = FeatureSet()
        fs2.add_feature(self.operand2)
        fs1_multi = fs1.multiply(multiplier, name_prefix, is_temporary)
        fs2_multi = fs2.multiply(multiplier, name_prefix, is_temporary)
        fs_result = self._internal_ops(fs1_multi, fs2_multi)
        return [fs1, fs2, fs1_multi, fs2_multi, fs_result]
