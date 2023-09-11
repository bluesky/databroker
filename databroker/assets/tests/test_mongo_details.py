from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pytest

from ..utils import install_sentinels


def test_double_sentinel(fs_mongo):
    fs = fs_mongo
    with pytest.raises(RuntimeError):
        install_sentinels(fs.config, fs.version)


def test_reconfigure(fs_mongo):
    fs = fs_mongo
    fs.reconfigure(fs.config)
