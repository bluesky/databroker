import copy
import pickle

import pytest
import json

from ..core import Document, NotMutable, Event


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


REALISTIC_EXAMPLE = Event({
    'descriptor': '007814d0-eb61-4f60-9a57-457e16be7427',
    'time': 1582483150.6633387,
    'data': {'det': 1.0},
    'timestamps': {'det': 1582483150.6561532},
    'seq_num': 1,
    'uid' : 'b1fb5021-303a-4c6d-8ffd-c3fe72f3a4d9',
    'filled': {}})


def test_repr():
    # Eval-ing the repr round-trips
    eval(repr(REALISTIC_EXAMPLE)) == REALISTIC_EXAMPLE
    # No newlines in the repr
    assert len(repr(REALISTIC_EXAMPLE).splitlines()) == 1


def test_repr_pretty():
    formatters = pytest.importorskip("IPython.core.formatters")
    f = formatters.PlainTextFormatter()
    display = f(REALISTIC_EXAMPLE)
    # Eval-ing the display round-trips
    eval(display) == REALISTIC_EXAMPLE
    # Mutli-line display for large documents
    assert len(display.splitlines()) > 1
