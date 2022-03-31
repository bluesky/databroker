import pickle

import pytest

from ..queries import TimeRange


def test_time_range():
    with pytest.raises(ValueError):
        # since must not be greater than until
        TimeRange(since='2021', until='2020')


def test_pickle():
    "Ensure that query objects are pickle-able."
    q = TimeRange(since='2020-01-01 9:52', timezone="US/Eastern")
    serialized = pickle.dumps(q)
    deserialized = pickle.loads(serialized)
    assert q == deserialized
