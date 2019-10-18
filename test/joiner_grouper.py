from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from framework.feature_factory import Feature, Feature_Factory, FeatureSet, ConfigObj
from framework.feature_factory.helpers import Helpers
from channelDemoStore import Store

# Example of joiners and groupers
store = Store(_snapshot_date="2002-01-31")
ff = store.ff
store.groupby_cols
store_features = store.Sales().get_all()
store_groupby_cols = store.groupby.store()
ff.append_features(store.config.get_or_else("cores.store_sales", None), store_groupby_cols, store_features)
