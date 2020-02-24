import copy
import pickle

import pytest
import json

from ..core import Document, NotMutable


def test_immutable():
    d = Document({'a': 1})
    with pytest.raises(NotMutable):
        # Update existing key
        d['a'] = 2
    with pytest.raises(NotMutable):
        # Add new key
        d['b'] = 2
    with pytest.raises(NotMutable):
        d.setdefault('a', 2)
    with pytest.raises(NotMutable):
        d.setdefault('b', 2)
    with pytest.raises(NotMutable):
        del d['a']
    with pytest.raises(NotMutable):
        d.pop('a')
    with pytest.raises(NotMutable):
        d.popitem()
    with pytest.raises(NotMutable):
        d.clear()
    with pytest.raises(NotMutable):
        # Update existing key
        d.update({'a': 2})
    with pytest.raises(NotMutable):
        # Add new key
        d.update({'b': 2})


def test_deep_copy():
    a = Document({'x': {'y': {'z': 1}}})
    b = copy.deepcopy(a)
    b['x']['y']['z'] = 2
    # Verify original is not modified.
    assert a['x']['y']['z'] == 1


def test_to_dict():
    a = Document({'x': {'y': {'z': 1}}})
    b = a.to_dict()
    assert type(b) is dict  # i.e. not Document
    b['x']['y']['z'] = 2
    # Verify original is not modified.
    assert a['x']['y']['z'] == 1


def test_pickle_round_trip():
    expected = Document({'x': {'y': {'z': 1}}})
    serialized = pickle.dumps(expected)
    actual = pickle.loads(serialized)
    assert type(actual) is Document
    assert actual == expected


def test_json_roundtrip():
    dd = Document({"x": {"y": {"z": 1}}})
    dd2 = json.loads(json.dumps(dd))
    assert dd == dd2


def test_msgpack_roundtrip():
    msgpack = pytest.importorskip("msgpack")
    dd = Document({"x": {"y": {"z": 1}}})
    dd2 = msgpack.loads(msgpack.dumps(dd), raw=False)
    assert dd == dd2
