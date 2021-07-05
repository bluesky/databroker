import collections.abc


class FactoryMap(collections.abc.Mapping):
    def __init__(self, factory):
        self._factory = factory

    def __getitem__(self, key):
        return self._factory()[key]

    def __len__(self):
        return len(self._factory())

    def __iter__(self):
        return iter(self._factory())

    def __repr__(self):
        return repr(self._factory())
