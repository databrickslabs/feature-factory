# Feature Factory

[![codecov](https://codecov.io/gh/databrickslabs/feature-factory/branch/master/graph/badge.svg)](https://codecov.io/gh/databrickslabs/feature-factory)

Feature factory simplifies the task of feature engineering by providing APIs built on top of PySpark with proper optimization, validation, and deduplication in mind. 
It is meant to be used as an accelerator for the organization to simplify and unify feature engineering workflow. The framework itself is just that - a framework. That is
it is not meant to stand alone. The accelerator must be forked and configured for your organization.

## Getting Started
1. Fork the repo
2. Replace channel with a concept relevant to your organization
3. Replace the Feature Family implementations for your organization (i.e. `ChannelDemoCatalog`)
4. Implement the Feature Families for each of your organization's concepts
5. Implement each metric within each Feature Family with the proper logic for the metric
6. Implement joiners, group_bys and multipliers as necessary

## An Example
All of the next few sections have more detailed documentation below. This section is meant as a high-level
overview of the way the different objects tie together and how they can be implemented for a specific use case. 

Please check out the [Demo](Demo) section where you'll find a Databricks Archive which includes everying needed
to get the example up and running including:
* TPC-DS generator scripts
* TPC-DS library
* Enhancement table scripts
* A detailed demo of how to use basic thru advanced features.


As with most complex concepts, the simplest way to understand is to review an example; that's specifically what this
repo is, an example of an implemented feature factory. The framework is a strong basis upon which additional concepts
can be easily built but it's meant to be fully extensible for your needs.

This example utilizes the [TPC-DS]() dataset to demonstrate how to implement the feature factory within a typical
sales environment. An implementation of a Feature Factory has a concept which implements many components from the
framework specific to said concept. In the example of TPC-DS, the concept of <b> "Channel"</b> has been implemented.
Channel was decided to be a concept since the fictitious organization behind TPC-DS utilizes three primary channels
of sales, Store, Web, and Catalog. Each of these three concepts are a type of channel, share many commonalities
but are significantly different in the way they are measured regarding time, geographical data, partitions, 
operations, metrics calculations, calendars, etc. Also, the data is quite different between the channels as well
and thus to spare all future data scientists and engineers from having to identify the relevant data, it will simply
be tied to the channel directly. As you can see from the image below, channel is not a part of the framework but
simply implements the framework appropriately based on the business rules of the concept.

### Usage
To use the feature factory (after it's been implemented for your specific case):
1. Instantiate the implemented concept
```python
# Instantiate store
store = Store(_snapshot_date="2002-01-31")
# Get the feature factory
ff = store.ff
```
2. Get your feature sets. If all the features you need are already defined in the feature families then it's 
very easy to get them
```python
# Get all sales features
# The following returns two feature sets, multipliable features and non-multipliable. When using multipliers
# (described later) only the multipliable features will be multiplied, base_features will not. An example of a
# base_feature would be something like "days_since_first_transaction" as this is a stagnant feature and one that 
# shouldn't be combined with other concepts.
mult_features, base_features = store.Sales().get_all()

# Get only a few features
# When getting features one-by-one an ordered dictionary of features is returned. This is so that features may easily
# be acquired from several feature families and later put together into a FeatureSet 
odict_of_features = store.Sales().netStoreSales().totalQuantity().get()
# Once you have your final dictionary[ies] of features put them all together into one or many FeatureSets
stores_sales_features = FeatureSet(odict_of_features)
# Or for many dicts of features
stores_sales_features = FeatureSet(*[odict_of_features, odict_of_features2])
```

3. Append your feature sets to a base dataframe. The base dataframe is usually a core or some custom-built df built to 
accomplish the need. Notice that the feature set is passed in as a list to allow for multiple feature sets to be 
appended in one append call.
```python
features_df = ff.append_features(store.get_core("store_sales"), ['ss_store_sk'], [stores_sales_features])
```

4. Makes as many feature_dfs as necessary, group them at different agg levels and join them together to create master
feature dataframes or whatever is needed.

### Data
Cores and Sources are types of data that are identified as dataframes relevant to the give concept. In this example,
`channelDemoStore` have a few relevant fact tables and many lookups; thus these are identified as relevant, curated
datasets for users wanting to work with the store channel. All canned metrics and features will utilize the
curated datasets to perform the necessary calculations.

### Feature Families
Just as channel implements Data it also implements features via Feature Families. A feature family is a logical
grouping of similar metrics. A perfect example is <b> Sales </b>. There are many, many different types of sales;
some examples include `weekend sales, gross sales, net sales, holiday sales, taxable sales, service sales, etc.`.
Each channel will likely calculate these differently but in this example, each channel has sales, so we group all
the different types of sales into a single feature family so that the similarities may easily be inherited and 
since the user will often need many metrics from the same grouping at one time. 

### Features
A feature is a specific object that maintains all the necessary metadata to calculate its value given some context.
For example, net store sales is defined:
```python
    @multipliable
    def netStoreSales(self,
                      _name="net_sales",
                      _base_col='ss_net_profit',
                      _filter=[F.col('ss_net_profit') > 0],
                      _negative_value=0,
                      _agg_func=F.sum): 
        return self
```
In English, this feature is the sum of the column `ss_net_profit` where `ss_net_profit` is greater than 0. If
the value is null or does not match the filter, replace the value with a 0. This is a very simple example of 
how a feature can be implemented. There are also more complex concepts described later such as how to ensure that
the referenced column exists in the relevant dataframe, but that's for later. The features agreed upon as, 
"valid" will be added directly to the feature factory deployment for the organization to utilize and will implement
the proper unit tests to ensure long-term accuracy.

In addition to features created inside the Feature Factory, Feature objects can be defined manually by a user allowing
data engineers and data scientists to rapidly prototype features and take advantage of all the powerful capabilities
such as feature multiplication.

### Feature Sets
Feature sets are `OrderedDicts{str:Feature`. Often times features will build off one another thus the order in 
which the features are created, stored and calculated becomes important, thus an ordered dictionary is used.
Feature Sets (basically lists of features) are what are passed into the action function of the Feature Factory,
`Append Features`. This is essentially the first and only spark action of the entire process and is responsible
for generating and building the final dataframe with all calculated features.

### Feature Factory - Append Features
The code below is a simple example of generating a dataframe that has all the calculated features contained
within feature set variables, `fs1 and fs2`. These will all be appended to some base dataframe, `df`. Since
the features are wanted at an aggregate level, the proper column names are passed in for grouping.
```python
featureDf = ff.append_features(df, groupBy_cols = ['ss_sold_date_sk', 'ss_store_sk'], feature_sets=[fs1, fs2])
```

### Config
The `ConfigObj` is an object that simplifies the transparency, reusability, and auditability of the Feature Factory.
The config also simplifies changes to complex configurations such as time ranges, join logic, etc. changes 
to the config are as simple as `config.add("key", "value")` where key is a string and value is anything. Typically,
something that can be referenced as a string is stored in the values like dictionaries, lists, etc. but more complex
objects can be stored as well if so desired.  

## More Detail
The following sections will explain the usage of Feature Factory in details.

## 1. The Channel Class 
The Channel class implements the Feature Factory's framework. This class must be created and customized 
based on the caveats, contingencies, and uniqueness of the data at hand. As this class is defined by the organization
it can be extended as much as necessary to accommodate the needs of its users.

For the remainder of this section an instance of Store will be created and used in examples. The Store object 
inherits from Channel and can be found in the channelDemoStore directory. `Store` will be used for examples. 
```python
from channelDemoStore import Store
store = Store(_snapshot_date=2001-12-31)
```

The snapshot date as referenced above is an extremely important concept for the Feature Factory. With analysis
and especially Machine Learning (ML) most concepts revolve around a date since many ML models are trained on
one set of data from one point in time and validated against future data and tested against further future data.
If a time based approach such as "snapshot_date" does not make sense for your organization, simple change the 
implementation to what's suitable for you. There will be additional changes needed to use some of the core
framework but it will become obvious as you begin to implement and the necessary changes should become obvious as well. 

### a. ConfigObj
ConfigObj is a container class of a dictionary, which simplifies access to the content of the config. 
You can access a config value from ConfigObj using the full path of the config key.
A default value can be provided in case the config key does not exist. The dictionary object can be accessed using config.configs. 
Configurations can be added in one of two ways - inherently through the partner class or at an ad-hoc basis outside of the partner class after it has been
instantiated. 

The config object is meant to: 
1. Enable faster onboarding through sharing of basic configs between teammates
2. Ensure repeatability between feature creation runs
3. Facilitate visibility into sometimes complex and hidden minutia (i.e. dates formats/ranges and join logic)

```python
from framework.configobj import ConfigObj
config = ConfigObj()
config.add("_partition_start", [201706]).add("_partition_end", [201812])
config.get_or_else("_partition_start", "")

# Easy add multiple layers deep
config.add("level1.newlevel2.config", "test_value")
config.add("level1.newlevel3", {"newDict":"adding_dict_vals"})
config.add("level1.simple.value", "simple_string_value")
config.add("level1.simple.list_value", ["this", "is", "a", "list", "of", "strings"])

# Pretty print the config settings
config.print()

# Drop single nested item
config.drop("level1.newlevel2.config")

# Export
dbutils.fs.rm("/tmp/mgm/ff/store/config")
config.export("/tmp/mgm/ff/store/config/demo_config", dbutils)

# Import
# The config can then be imported and used to instantiate concepts such as Store
from channelDemoStore import Store
imported_config = config.import("/tmp/mgm/ff/store/config/demo_config")
store = Store(_snapshot_date=2001-12-31, _config=)
```

#### b. Data
Within feature factory, data is classified as either a core or a source. Cores are generally well-formed fact tables
and sources are the slow-changing dimensions, lookups, etc. that facilitate the facts. Data can be simplified if both 
concepts are not necessary such that only cores or only sources are used. There is no physical differentiation in the code
it's merely a logical construct to help users differentiate between well-formed and curated data vs lookup data,
custom data, or manually maintained data. Obviously, these can be used/implemented in whatever way is fitting for
the organization. 

Data typically ties to a specific implementation of the concept from which it inherits. In this example, very 
different data is used for Store and Catalog channels; thus the relevant data that is curated for the implemented 
concept will be tied to the Store/Channel/etc.

Note that data objects (cores/sources) are automatically filtered down by partition to whatever is manually specified
or the derived defaults. This is very powerful for performance, as when configured properly, only the necessary partitions
will be considered when the feature factory is used.


```python
# get a dictionary of core names and dataframes
from channelDemoStore import Store
store = Store(_snapshot_date=2001-12-31)
store.cores

# list core/source names
store.list_cores()
store.list_sources()# get the dataframe of table sales_item_fact
store.get_core("store_sales") # Returns dataframe

```
Similar to configs, data (both sources and cores) can be added in two ways:


Directly to the channel's Store class:
```python
    def _create_default_sources(self):
        try:
            df1 = spark.read.table("databse.table")
            self.add_source("item", df1, ["partition_col1", "partition_col2"])
            df2 = anyDF # use any spark reader to define a dataframe here

        except Exception as e:
            logger.warning("Error loading default sources. {}".format(str(e)))
            traceback.print_exc(file=sys.stdout)

        return self.sources
```
or 
at an ad-hoc basis after the partner class has been instantiated:

```python
item_df = spark.read.table("db.item")
store.add_core('store_returns', item_df, partition_col=[sr_returned_date_sk])
```

## 2. Building out the Feature Family 
Once the channel class has been built out to include the initialization of the config, data, and/or any other additional
customizations, the third layer (sitting atop the framework and channel class) is a Feature Family class where features,
joiners, and groupers can be defined. Similar to the layout of the partner class, there is ample room for additions beyond what
is outlined in this document. 
#### a. Base Features
Base features are defined within this class. These are the features that will be multiplied out across time multipliers or categorical 
multipliers. Base features themselves have a variety of parameters, however at it's most basic form, a feature defines a base column
filter (optional), and an aggregate function. Below is a full list of attributes:
* _name: name of the feature
* _base_col: the column(s) from which the feature is derived when the filter is true or there is no filter.
            Note base_col is of type column meaning it can be compiled such as
```python
base_col = F.concat(F.col("POS_TRANS_ID"), F.col("TRANS_DATE"), F.col("STORE_ID"), F.col("POS_TILL_NUM")
``` 
* _filter: the condition to choose base_col or negative_value
* _negative_value: the value the feature is derived from when the filter condition is false
* _agg_func: the aggregation functions for computing the feature value from base_col or negative_value
* _agg_alias: alias name
* _joiners: config of table joining for this feature
* _kind: Is the feature `multipliable` (default) or `base`. For example, a feature is multipliable if it should 
be allowed to be combined with other concepts through the use of a multiplier. Some features such as 
`daysSinceFirstTransaction` are base features and should not be multiplied since they are static.
* _is_temporary: Some features are used only to as building blocks for other features and should not be appended
to the final output as a true feature. In this case, switch this flag to True and these intermediate features will
be culled before the final dataframe is delivered.

Similar to both the config and data, base features can be added in two ways:
Natively within the class:
```python
import pyspark.sql.functions as F
    @multipliable
    def netStoreSales(self,
                      _name="net_sales",
                      _base_col='ss_net_profit',
                      _filter=[F.col('ss_net_profit') > 0]
                      _negative_value=0,
                      _agg_func=F.sum):
        self._create_feature(inspect.currentframe())
        return self
```
Note that the feature above utilizes the `multipliable` decorator meaning that, using multipliers, additional 
features can be generated such as netStoreSales_12m. 


Additionally base features can be created in an ad-hoc fashion and appended to any Feature Set such that the 
`ff.append_features` method can append custom features. 
```python
from pyspark.sql.functions import col 
import pyspark.sql.functions as F 
  Feature(_name = 'total_custs', 
         _base_col= col('ss_customer_sk'),
         _agg_func = F.approx_count_distinct)

``` 
Beyond defining a feature as a base column with an aggregate function, composite features that reference previously
defined features can also be defined. Below netStoreSales and and totalQuantity are features that are already
defined; their outputs create `net_sales` and `total_quantity` columns respectively...thus, they can be referenced
in a newly defined feature so long as those necessary features are created before the `_create_feature` function
is called.   
```python
import pyspark.sql.functions as F
    @multipliable
    def netSalesPerQuant(self,
                         _name='net_sales_per_quantity',
                         _base_col = F.col('net_sales') / F.col('total_quantity'),
                         _filter=[],
                         _col_alias=None,
                         _negative_value=0,
                         _kind=""):
        self.netStoreSales().totalQuantity()
        self._create_feature(inspect.currentframe())
        return self
```

<b> NOTE: </b> the decorator for non-multipliable base features is:

```python
    @base_feat
    def featurefunc(...)
```



Feature families have a set of predefined base features, which can be selected by chaining or calling `get_all()`
The `get_all()` method returns two values, multipliable and base feature sets.

```python
# Get all features from sales as a dictionary
multipliable_sales_featuers, base_sales_features = store.Sales().get_all()

output: {(<framework.feature_factory.feature.FeatureSet at 0x7f5953c9b898>,
 <framework.feature_factory.feature.FeatureSet at 0x7f5953c9b748>)}

# Features can be selected by chaining:
my_sales_features = store.Sales().netStoreSales().totalQuantity().netSalesPerQuant().get()
```
Custom defined (e.g. ad-hoc) features can be created and appended to pre-existing Feature Sets. The following 
example defines a feature which estimates the number of transactions and appends it to a previously defined
Feature Set, `my_sales_features`. 
```python
# Ad Hoc feature
from pyspark.sql import functions as F
from framework.feature_factory.feature import Feature
my_sales_features["total_transactions"] = (
  Feature(_name="total_transactions",
          _base_col=F.col("POS_TRANS_ID"),
          _negative_value="",
          _agg_func=F.approx_count_distinct
         )
)
```

A few tips in utilizing these attributes: 
base_col can be a literal. This example creates a feature which counts the number of items in last 12 months. 
The condition (last 12 months) is defined as a filter. If the condition is true, value of base_col will be 
selected (1), else the negative_value(0). The `feature_filter` can be a single filter (a column type that returns
True/False is considered a filter) or a list of filters. This becomes very powerful as many features instances
can be created and appended to a Feature Set from within a complex loop of loops...
```python
import pyspark.sql.functions as F
feature_filter = F.col("sr_returned_date_sk").between(201706, 201805)
features["store_number_of_items_{}".format(t)] = (Feature(_name="STORE_NumberOfItems_12m",
                          _base_col=lit(1),
                          _filter=feature_filter,
                          _negative_value=0,
                          _agg_func=F.sum
                          )
                  )
```
The filter is a boolean expression. Here are a few more examples of filters:
```python
filter = col("PARTNER_OFFER_TYPE_CATEGORY").isin('BONUS')
filter = col("CUSTOMER_ID") > 0
```
The base col can be a combination of multiple columns.
```python
import pyspark.sql.functions as F 
columns = [F.col("sr_return_time"), F.col("sr_ticket_number"), F.col("sr_item_sk")]
base_col = F.concat(*columns)
```
Another useful attribute of a Feature object is output_alias, which gives the column name of the feature after 
it is appended to a dataframe. To run a query for the result dataframe, output_alias can be applied as following:
```python
base_feature_cols = []
for feature in master_features.values():
  if "3m" in feature.output_alias:
    base_feature_cols.append(feature.output_alias)
display(features_target_df.select(*[col("COLLECTOR_NUMBER")], *base_feature_cols, *[col("TARGET")]))
```

#### b. Joiners
Class Joiner defines how a dataframe will be joined to the primiary data source. When creating a Data instance, joiners can be added to the Data object.
For now a joiner is meant to join lookups and other simple tables for the purpose of generating a feature on
the same aggregate level.

A joiner is a class object which contains joining logic including
* DataFrame to be joined with the primary data source.
* The join conditions
* Join type (i.e. inner, left outer, etc.)

Joiners are defined in the Feature Family and added to the primary data source. Below is the definition
of a joiner that will join the store dimension to store_sales_df assuming the source df contains `ss_store_sk`. 
```python
    store_joiner = Joiner(store_df, on=F.col("ss_store_sk") == F.col('s_store_sk'), how="inner")
    self.add_source("store_sales", store_sales_df, ["p_yyyymm"], joiners=[store_joiner]
```
Refer to `channelDemoStore.sales.py` to see specific implementation of joiners.

## 4. Multipliers & Time Management
Multipliers are very powerful as they combine multiple concepts/ideas together to derive complex, derived features.
Categorical and Time multipliers come out of the box as well as a Trends Feature Family which utilizes the Time
Multiplier to build trend lines and projections. 

Note there is a `dtm` (Date_Time_Manager) that is responsible for building and maintaining the `time_helpers` section of 
the config. It's important that you become very familiar with this section of the config as it determines how all the time, 
filters, and multipliers are implemented.
```python
store.config.get_config("time_helpers").configs
``` 
```text
{'snapshot_type': 'MONTH',
 'snapshot_date': '2002-01-31',
 'partition_col': 'p_yyyymm',
 'date_col': 'd_date',
 'date_col_format': '%Y-%m-%d',
 'partition_col_format': '%Y%m',
 'date_filters': {'ranges': {'1m': {'start': '2001-12-31',
    'end': '2002-01-31'},
   '3m': {'start': '2001-10-31', 'end': '2002-01-31'},
   '6m': {'start': '2001-07-31', 'end': '2002-01-31'},
   '12m': {'start': '2001-01-31', 'end': '2002-01-31'}}},
 'partition_lower': '200101',
 'partition_upper': '200201'}
```

Multiplied by the time helper above, base feature "netStoreSales" will generate "STORE_net_sales_1m", "STORE_net_sales_3m", 
"STORE_net_sales_6m", "STORE_net_sales_12m", and "STORE_net_sales_9m" as derived features.

Here is an example of creating a time multiplier and deriving time-base features from multiplication.
```python
store_features, _ = store.Sales().get_all()
time_multipliers = store.get_daterange_multiplier()
ex = store_features.multiply(time_multipliers, "STORE")
print(ex.features.keys())

output:odict_keys(['STORE_1m_net_sales', 'STORE_3m_net_sales', 'STORE_6m_net_sales', 'STORE_9m_net_sales', 'STORE_12m_net_sales', 
'STORE_1m_total_quantity', 'STORE_3m_total_quantity', 'STORE_6m_total_quantity', 'STORE_9m_total_quantity', 'STORE_12m_total_quantity', 
'STORE_1m_total_custs', 'STORE_3m_total_custs', 'STORE_6m_total_custs', 'STORE_9m_total_custs', 'STORE_12m_total_custs'])
```
Beyond using time based multipliers, Feature Factory also enables the multiplication of multipliable base features by categorical variables.
This enables rapid generation of derived features by both time and categorical multipliers. 
Note there are convenient Helper functions that can be useful in retrieving categorical 
columns in both cores and sources.  
```python
categorical_multipliers = Helpers().get_categoricals_multiplier(df = store.get_source('item'), col_list = ['i_color', 'i_category'], ignore_cols = [])
ex2 = ex.multiply(categorical_multipliers, 'STORE')

``` 
## 5. Feature Dictionary

In the reference implementation, a module is implemented as a Feature Family (a collection of features). A read-only property is defined for each feature to provide easy access. A feature family extends an ImmutableDictBase class, which is generic and can serve as base class for collections of features, filters and other objects. In the code example below, filter definitions are extracted from features and form a separate Filters class. The common features shared by multiple families are also extracted into a separate Common Features class for reuse. Both filters and common features are inherited by the StoreSales family class, which defines a new set of features based upon the common definitions.

In the code example, there is only one channel; multiple channels share the same CommonFeatures. Retrieving a feature definition from a specific channel is as simple as accessing a property of that family class, e.g. store_channel.total_sales

Here is an code example of feature dictionary implemented using ImmutableDictBase

```python
class CommonFeatures(ImmutableDictBase):
    def __init__(self):
        self._dct["customer_id"] = Feature(_name="customer_id", _base_col=f.col("ss_customer_sk"))
        self._dct["trans_id"] = Feature(_name="trans_id", _base_col=f.concat("ss_ticket_number","d_date"))

    @property
    def collector(self):
        return self._dct["customer_id"]

    @property
    def trans_id(self):
        return self._dct["trans_id"]


class Filters(ImmutableDictBase):
    def __init__(self):
        self._dct["valid_sales"] = f.col("ss_net_paid") > 0

    @property
    def valid_sales(self):
        return self._dct["valid_sales"]


class StoreSales(CommonFeatures, Filters):
    def __init__(self):
        self._dct = dict()
        CommonFeatures.__init__(self)
        Filters.__init__(self)

        self._dct["total_trans"] = Feature(_name="total_trans",
                                           _base_col=self.trans_id,
                                           _filter=[],
                                           _negative_value=None,
                                           _agg_func=f.countDistinct)

        self._dct["total_sales"] = Feature(_name="total_sales",
                                           _base_col=f.col("ss_net_paid").cast("float"),
                                           _filter=self.valid_sales,
                                           _negative_value=0,
                                           _agg_func=f.sum)

    @property
    def total_sales(self):
        return self._dct["total_sales"]

    @property
    def total_trans(self):
        return self._dct["total_trans"]
```

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.
