from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import time as ttime
import datetime
import uuid

from nose.tools import (assert_equal, assert_in, raises)
from nose import SkipTest
from ..doc import (Document, pretty_print_time,
                   DocumentIsReadOnly, ref_doc_to_uid)

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


@raises(DocumentIsReadOnly)
def test_doc_descructive_pop():
    src_str, dd, doc_test = _syn_data_helper()
    k = next(doc_test.keys())
    doc_test.pop(k)


@raises(DocumentIsReadOnly)
def test_doc_descructive_del():
    src_str, dd, doc_test = _syn_data_helper()
    k = next(doc_test.keys())
    del doc_test[k]


@raises(DocumentIsReadOnly)
def test_doc_descructive_delattr():
    src_str, dd, doc_test = _syn_data_helper()
    k = next(doc_test.keys())
    delattr(doc_test, k)


@raises(DocumentIsReadOnly)
def test_doc_descructive_setitem():
    src_str, dd, doc_test = _syn_data_helper()
    k = next(doc_test.keys())
    doc_test[k] = 'aardvark'


@raises(DocumentIsReadOnly)
def test_doc_descructive_setattr():
    src_str, dd, doc_test = _syn_data_helper()
    k = next(doc_test.keys())
    setattr(doc_test, k,  'aardvark')


@raises(DocumentIsReadOnly)
def test_doc_descructive_update():
    src_str, dd, doc_test = _syn_data_helper()
    doc_test.update(dd)


def test_html_smoke():
    src_str, dd, doc_test = _syn_data_helper()

    try:
        doc_test._repr_html_()
    except ImportError:
        raise SkipTest("Missing imports for html repr")


def test_round_trip():
    src_str, dd, doc_test = _syn_data_helper()
    name, doc_dict = doc_test.to_name_dict_pair()
    assert_equal(name, doc_test['_name'])
    assert_equal(name, doc_test._name)
    assert_equal(dd, doc_dict)
    new_doc = Document(name, doc_dict)
    assert_equal(doc_test, new_doc)


def test_ref_to_uid():
    a = Document('animal', {'uid': str(uuid.uuid4()),
                            'animal': 'arrdvark'})
    b = Document('zoo', {'name': 'BNL Zoo',
                         'prime_attraction': a})
    b2 = ref_doc_to_uid(b, 'prime_attraction')
    assert_equal(b2['prime_attraction'], a['uid'])
    assert_equal(b['prime_attraction'], a)


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
