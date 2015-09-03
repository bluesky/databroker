from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import time as ttime
import datetime

from nose.tools import (assert_equal, assert_in, assert_not_in)
from nose import SkipTest
from ..doc import Document, pretty_print_time

import logging
loglevel = logging.DEBUG
logger = logging.getLogger(__name__)
logger.setLevel(loglevel)
handler = logging.StreamHandler()
handler.setLevel(loglevel)
logger.addHandler(handler)

# some useful globals
run_start_uid = None
document_insertion_time = None
descriptor_uid = None
run_stop_uid = None


def _syn_data_helper():
    src_str = 'ABCDEFGHI'
    dd = {k: ord(k) for k in src_str}
    doc_test = Document('testing', dd)
    return src_str, dd, doc_test


def test_doc_plain():
    src_str, dd, doc_test = _syn_data_helper()
    assert_equal('testing', doc_test._name)

    assert_equal(set(doc_test.keys()),
                 set(dd.keys()))
    for k in doc_test:
        assert_equal(doc_test[k], ord(k))
        assert_equal(dd[k], doc_test[k])
        assert_equal(getattr(doc_test, k), doc_test[k])
        assert_in(k, doc_test)

    for k, v in doc_test.items():
        assert_equal(v, ord(k))

    assert_equal(set(dd.values()), set(doc_test.values()))


def test_doc_descructive_pop():
    src_str, dd, doc_test = _syn_data_helper()
    for k in src_str:
        ret = doc_test.pop(k)
        assert_equal(ret, ord(k))
        assert_not_in(k, doc_test)
        assert_in(k, dd)
    print(doc_test)
    assert_equal(len(doc_test), 0)


def test_doc_descructive_del():
    src_str, dd, doc_test = _syn_data_helper()

    for k in src_str:
        del doc_test[k]
        assert_not_in(k, doc_test)
        assert_in(k, dd)
    assert_equal(len(doc_test), 0)


def test_doc_descructive_delattr():
    src_str, dd, doc_test = _syn_data_helper()

    for k in src_str:
        delattr(doc_test, k)
        assert_not_in(k, doc_test)
        assert_in(k, dd)
    assert_equal(len(doc_test), 0)


def test_html_smoke():
    src_str, dd, doc_test = _syn_data_helper()

    try:
        doc_test._repr_html_()
    except ImportError:
        raise SkipTest("Missing imports for html repr")


def test_pprint_time():
    offset_seconds = 50
    target = '{off} seconds ago ({dt})'
    test = ttime.time() - offset_seconds
    try:
        res = pretty_print_time(test)
    except ImportError:
        raise SkipTest("Missing imports for pretty print time")
    dt = datetime.datetime.fromtimestamp(test).isoformat()
    expt = target.format(off=offset_seconds, dt=dt)
    assert_equal(res, expt)

if __name__ == '__main__':
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)
