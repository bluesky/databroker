from ..utils import LazyMap

def test_lazymap():

    loaded = {'A': 0, 'B': 0}

    class TestClass():
        def __init__(self, value):
            self.value = value
            loaded[value] += 1

    lazy_map = LazyMap({'A': lambda: TestClass('A'),
                        'B': lambda: TestClass('B')})

    assert loaded['A'] is 0
    assert loaded['B'] is 0

    assert isinstance(lazy_map['A'], TestClass)

    assert loaded['A'] is 1
    assert loaded['B'] is 0

    assert isinstance(lazy_map['B'], TestClass)

    assert loaded['A'] is 1
    assert loaded['B'] is 1

    lazy_map['A']
    lazy_map['B']

    assert loaded['A'] is 1
    assert loaded['B'] is 1


def test_lazymap_contains():
    loaded = {'A': 0, 'B': 0}

    class TestClass():
        def __init__(self, value):
            self.value = value
            loaded[value] += 1

    lazy_map = LazyMap({'A': lambda: TestClass('A'),
                        'B': lambda: TestClass('B')})

    assert loaded['A'] is 0
    assert loaded['B'] is 0

    'A' in lazy_map
    'B' in lazy_map

    assert loaded['A'] is 0
    assert loaded['B'] is 0

