from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pytest

from ..utils import install_sentinels


def test_double_sentinel(fs_mongo):
    fs = fs_mongo
    with pytest.raises(RuntimeError):
        install_sentinels(fs.config, fs.version)


def test_index(fs_mongo):
    fs = fs_mongo
    fs._create_datum_index()
    indx = fs._datum_col.index_information()

    assert len(indx) == 3
    index_fields = set(v['key'][0][0] for v in indx.values())
    assert index_fields == {'_id', 'datum_id', 'resource'}


def test_reconfigure(fs_mongo):
    fs = fs_mongo
    fs.reconfigure(fs.config)
