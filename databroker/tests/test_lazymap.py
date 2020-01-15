from ..utils import LazyMap

def test_lazymap():

    loaded = {'A': False, 'B': False}

    class TestClass():
        def __init__(self, value):
            self.value = value
            loaded[value] = True

    lazy_map = LazyMap({'A': lambda: TestClass('A'),
                        'B': lambda: TestClass('B')})

    assert loaded['A'] is False
    assert loaded['B'] is False

    assert isinstance(lazy_map['A'], TestClass)

    assert loaded['A'] is True
    assert loaded['B'] is False

    assert isinstance(lazy_map['B'], TestClass)

    assert loaded['A'] is True
    assert loaded['B'] is True
