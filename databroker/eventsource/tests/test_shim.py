from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from databroker.eventsource.tests.utils import (build_shim_from_init)


def test_name():
    ess = build_shim_from_init()
    assert ess.name == 'mds'
