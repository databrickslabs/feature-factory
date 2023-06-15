from enum import IntEnum, EnumMeta
from pyspark.sql.column import Column

class AggregationGranularity:
    def __init__(self, granularity: EnumMeta) -> None:
        assert isinstance(granularity, EnumMeta), "Granularity should be of type Enum."
        self.granularity = granularity


    def validate(self, feat, groupby_list):
        if not feat.agg_granularity: 
            return None
        min_granularity_level = float("inf")
        for level in groupby_list:
            if isinstance(level, str):
                try:
                    level = self.granularity[level]
                except:
                    print(f"{level} is not part of {self.granularity}")
                    continue
            if isinstance(level, Column):
                continue
            min_granularity_level = min(min_granularity_level, level.value)
        assert min_granularity_level <= feat.agg_granularity.value, f"Required granularity for {feat.name} is {feat.agg_granularity}"
    
        