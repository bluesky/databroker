import functools
import pickle

import pytest

from ..utils import LazyMap


def test_lazymap():

    loaded = {'A': 0, 'B': 0}

    class TestClass():
        def __init__(self, value):
            self.value = value
            loaded[value] += 1

    lazy_map = LazyMap({'A': lambda: TestClass('A'),
                        'B': lambda: TestClass('B')})

    assert loaded['A'] == 0
    assert loaded['B'] == 0

    assert isinstance(lazy_map['A'], TestClass)

    assert loaded['A'] == 1
    assert loaded['B'] == 0

    assert isinstance(lazy_map['B'], TestClass)

    assert loaded['A'] == 1
    assert loaded['B'] == 1

    lazy_map['A']
    lazy_map['B']

    assert loaded['A'] == 1
    assert loaded['B'] == 1


def test_lazymap_contains():
    loaded = {'A': 0, 'B': 0}

    class TestClass():
        def __init__(self, value):
            self.value = value
            loaded[value] += 1

    lazy_map = LazyMap({'A': lambda: TestClass('A'),
                        'B': lambda: TestClass('B')})

    assert loaded['A'] == 0
    assert loaded['B'] == 0

    'A' in lazy_map
    'B' in lazy_map

    assert loaded['A'] == 0
    assert loaded['B'] == 0

def test_lazymap_add():

    class TestClass():
        def __init__(self, value):
            self.value = value

    lazy_map = LazyMap({'A': lambda: TestClass('A'),
                        'B': lambda: TestClass('B')})

    with pytest.raises(TypeError):
        lazy_map.add({'A': lambda: TestClass('A')})

    with pytest.raises(TypeError):
        lazy_map.add({'B': lambda: TestClass('B')})

    lazy_map.add({'C': lambda: TestClass('C')})
    assert 'C' in lazy_map


def f(x):
    return x


def test_lazymap_pickle():
    lazy_map = LazyMap({'x': functools.partial(f, 1)})
    serialized = pickle.dumps(lazy_map)
    deserialized = pickle.loads(serialized)
    assert lazy_map == deserialized
    expected = {'x': 1}
    assert dict(lazy_map) == expected
    assert dict(deserialized) == expected
