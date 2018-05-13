from __future__ import absolute_import, division, print_function
import copy

from databroker import Header
# do this as a weird import to get the py2 shim
from databroker._core import SimpleNamespace


def test_header_dict_conformance(db):
    db.prepare_hook = lambda name, doc: copy.deepcopy(doc)

    # TODO update this if / when we add conformance testing to
    # validate attrs in Header
    target = {'start': {'uid': 'start'},
              'stop': {'uid': 'stop', 'start_uid': 'start'},
              'ext': SimpleNamespace()}

    h = Header(db, **target)
    # hack the descriptor lookup/cache mechanism
    target['descriptors'] = [{'uid': 'desc', 'start_uid': 'start'}]
    h._cache['desc'] = [{'uid': 'desc', 'start_uid': 'start'}]

    assert len(h) == len(target)
    assert set(h) == set(target)
    assert set(h.keys()) == set(target.keys())

    for k, v in h.items():
        assert v == target[k]
        assert v == h[k]

    # this is a dumb test
    assert len(list(h.values())) == len(h)

    n, d = h.to_name_dict_pair()
    assert n == 'header'
    assert d == target
