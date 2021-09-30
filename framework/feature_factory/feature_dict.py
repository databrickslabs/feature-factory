from collections.abc import Mapping
import copy


class ImmutableDictBase(Mapping):
    def __init__(self):
        pass

    def __getitem__(self, item):
        return copy.deepcopy(self._dct[item])

    def __iter__(self):
        return iter(self._dct)

    def __len__(self):
        return len(self._dct)