from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six
import pandas as pd
import numpy as np
import pytest


@pytest.fixture()
def lh_registry(request, registry):
    class LocalHandler(object):
        def __init__(self, rpath):
            pass

        def __call__(self, a, b):
            return {'a': a, 'b': b}

    registry.register_handler('test', LocalHandler)
    return registry


def _verify_datums(d_ids, tbl, reg):
    tbl = pd.DataFrame(tbl)
    for d, (i, r) in zip(d_ids, tbl.iterrows()):
        ret = reg.retrieve(d)
        assert ret == dict(r)


def _datasource(func):
    N = 100
    df = pd.DataFrame({'a': np.arange(N),
                       'b': np.random.randint(24**2, size=N)})
    data = [df, {k: v.values for k, v in six.iteritems(df)}]
    return pytest.mark.parametrize('dd', data, ids=['table', 'dict'])(func)


@_datasource
def test_bulk_datum_register_table(lh_registry, dd):
    registry = lh_registry

    r = registry.register_resource('test', '', '', {})
    d_ids = registry.bulk_register_datum_table(r, dd)
    _verify_datums(d_ids, dd, registry)


@_datasource
def test_single_datum_register(lh_registry, dd):
    registry = lh_registry
    N = 100

    dd = pd.DataFrame({'a': np.arange(N),
                       'b': np.random.randint(24**2, size=N)})
    r = registry.register_resource('test', '', '', {})
    d_ids = [registry.register_datum(r, dict(row))
             for i, row in dd.iterrows()]
    _verify_datums(d_ids, dd, registry)


def test_pkg_resources():
    from databroker.assets.base_registry import BaseRegistryRO
