from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time as ttime
import datetime

import pytz
from nose.tools import assert_equal, assert_raises, raises
import metadatastore.commands as mdsc
from metadatastore.utils.testing import mds_setup, mds_teardown
from metadatastore.examples.sample_data import temperature_ramp


# some useful globals
blc_uid = None
run_start_uid = None
document_insertion_time = None


#### Nose setup/teardown methods ###############################################


def teardown():
    mds_teardown()


def setup():
    mds_setup()
    global blc_uid, run_start_uid, document_insertion_time
    document_insertion_time = ttime.time()
    temperature_ramp.run()
    blc_uid = mdsc.insert_beamline_config({}, time=document_insertion_time)
    run_start_uid = mdsc.insert_run_start(scan_id=3022013,
                                          beamline_id='testbed',
                                          beamline_config=blc_uid,
                                          owner='tester',
                                          group='awesome-devs',
                                          project='Nikea',
                                          time=document_insertion_time)


#### Testing metadatastore insertion functionality #############################


@raises(KeyError)
def check_for_id(document):
    """Make sure that our documents do not have an id field

    Parameters
    ----------
    document : metadatastore.document.Document
        A sanitized mongoengine document
    """
    document['id']


def _blc_tester(config_dict):
    """Test BeamlineConfig Insert
    """
    blc_uid = mdsc.insert_beamline_config(config_dict, ttime.time())
    blc_mds, = mdsc.find_beamline_configs(uid=blc_uid)
    # make sure the beamline config document has no id
    check_for_id(blc_mds)
    assert_equal(blc_mds.uid, blc_uid)
    if config_dict is None:
        config_dict = dict()
    assert_equal(config_dict, blc_mds.config_params)
    return blc_mds


def test_beamline_config_insertion():
    for cfd in [None, {}, {'foo': 'bar', 'baz': 5, 'biz': .05}]:
        yield _blc_tester, cfd


def test_event_descriptor_insertion():
    # format some data keys for insertion
    data_keys = {'some_value': {'source': 'PV:pv1',
                                'shape': [1, 2],
                                'dtype': 'array'},
                 'some_other_val': {'source': 'PV:pv2',
                                    'shape': [],
                                    'dtype': 'number'},
                 'data_key3': {'source': 'PV:pv1',
                               'shape': [],
                               'dtype': 'number',
                               'external': 'FS:foobar'}}
    time = ttime.time()
    # test insert
    ev_desc_uid = mdsc.insert_event_descriptor(run_start_uid, data_keys, time)
    ev_desc_mds, = mdsc.find_event_descriptors(uid=ev_desc_uid)
    # make sure the sanitized event descriptor has no uid
    check_for_id(ev_desc_mds)

    # make sure the event descriptor is pointing to the correct run start
    referenced_run_start = ev_desc_mds.run_start
    assert_equal(referenced_run_start.uid, run_start_uid)
    assert_equal(ev_desc_mds['time'], time)

    for k in data_keys:
        for ik in data_keys[k]:
            assert_equal(getattr(ev_desc_mds.data_keys[k], ik),
                         data_keys[k][ik])


@raises(mdsc.EventDescriptorIsNoneError)
def test_ev_insert_fail():
    """Make sure metadatastore correctly barfs if an event is inserted
    with no event descriptor
    """
    mdsc.insert_event(None, ttime.time(), data={'key': [0, 0]}, seq_num=0)


def test_insert_run_start():
    time = ttime.time()
    beamline_id = 'sample_beamline'
    scan_id = 42
    custom = {'foo': 'bar', 'baz': 42,
            'aardvark': ['ants', 3.14]}
    run_start_uid = mdsc.insert_run_start(
        time, beamline_id=beamline_id, beamline_config=blc_uid,
        scan_id=scan_id, custom=custom)

    run_start_mds, = mdsc.find_run_starts(uid=run_start_uid)

    names = ['time', 'beamline_id', 'scan_id'] + list(custom.keys())
    values = [time, beamline_id, scan_id] + list(custom.values())

    for name, val in zip(names, values):
        assert_equal(getattr(run_start_mds, name), val)

    assert_equal(blc_uid, run_start_mds.beamline_config.uid)
    # make sure the metadatstore document raises properly
    check_for_id(run_start_mds)


def test_run_stop_insertion():
    """Test, uh, the insertion of run stop documents
    """
    run_start_uid = mdsc.insert_run_start(
        time=ttime.time(), beamline_id='sample_beamline', scan_id=42,
        beamline_config=blc_uid)
    time = ttime.time()
    exit_status = 'success'
    reason = 'uh, because this is testing and it better be a success?'
    # insert the document
    run_stop_uid = mdsc.insert_run_stop(run_start_uid, time,
                                        exit_status=exit_status,
                                        reason=reason)

    # get the sanitized run_stop document from metadatastore
    run_stop, = mdsc.find_run_stops(uid=run_stop_uid)

    # make sure it does not have an 'id' field
    check_for_id(run_stop)
    # make sure the run stop is pointing to the correct run start
    referenced_run_start = run_stop.run_start
    assert_equal(referenced_run_start.uid, run_start_uid)

    # check the remaining fields
    comparisons = {'time': time,
                   'exit_status': exit_status,
                   'reason': reason,
                   'uid': run_stop_uid}
    for attr, known_value in comparisons.items():
        assert_equal(known_value, getattr(run_stop, attr))


#### Testing misc metadatastore functionality ##################################


@raises(ValueError)
def test_proper_data_format():
    """Make sure metadatastore correctly barfs if the values of the data
    dictionary are not formatted as a twople of (value, timestamp)
    """
    data = {'key': [15, ]}
    mdsc._validate_data(data)


def test_dict_key_replace_rt():
    """Ensure metadatastore deals with dots in potential data keys correctly
    """
    test_d = {'a.b': 1, 'b': .5, 'c.d.e': None}
    src_in, dst_in = mdsc._src_dst('in')
    test_d_in = mdsc._replace_dict_keys(test_d, src_in, dst_in)
    src_out, dst_out = mdsc._src_dst('out')
    test_d_out = mdsc._replace_dict_keys(test_d_in, src_out, dst_out)
    assert_equal(test_d_out, test_d)


def test_src_dst_fail():
    assert_raises(ValueError, mdsc._src_dst, 'aardvark')


#### Testing metadatastore find functionality ##################################
def _find_helper(func, kw):
    func(**kw)


def test_find_funcs_for_smoke():
    """ Exercise documented kwargs in the find_* functions
    """
    rs, = mdsc.find_run_starts(uid=run_start_uid)
    test_dict = {
        mdsc.find_run_starts: [
            {'limit': 5},
            {'start_time': ttime.time()},
            {'start_time': '2015'},
            {'start_time': '2015-03-30'},
            {'start_time': '2015-03-30 03:00:00'},
            {'start_time': datetime.datetime.now()},
            {'stop_time': ttime.time()},
            {'start_time': ttime.time() - 1, 'stop_time': ttime.time()},
            {'beamline_id': 'csx'},
            {'project': 'world-domination'},
            {'owner': 'drdrake'},
            {'scan_id': 1},
            {'uid': run_start_uid}],
        mdsc.find_run_stops: [
            {'start_time': ttime.time()},
            {'stop_time': ttime.time()},
            {'start_time': ttime.time()-1, 'stop_time': ttime.time()},
            {'reason': 'whimsy'},
            {'exit_status': 'success'},
            {'uid': 'foo'}],
        mdsc.find_event_descriptors: [
            {'run_start': rs},
            {'run_start_uid': rs.uid},
            {'start_time': ttime.time()},
            {'stop_time': ttime.time()},
            {'start_time': ttime.time() - 1, 'stop_time': ttime.time()},
            {'uid': 'foo'}],
        mdsc.find_run_stops: [
            {'run_start': rs},
            {'run_start_uid': rs.uid},
        ]
    }
    for func, list_o_dicts in test_dict.items():
        for dct in list_o_dicts:
            yield _find_helper, func, dct


#todo this one...
def test_find_funcs_for_accuracy():
    pass


#### Test metadatastore time formatting ########################################


def _normalize_human_friendly_time_tester(val, should_succeed, etype):
    if isinstance(val, tuple):
        (val, check_output) = val

    if should_succeed:
        output = mdsc._normalize_human_friendly_time(val)
        assert(isinstance(output, float))
        try:
            assert_equal(output, check_output)
        except NameError:
            pass
    else:
        assert_raises(etype, mdsc._normalize_human_friendly_time, val)


def test_normalize_human_friendly_time():
    # should get tz from conf?  but no other tests get conf stuff...
    zone = pytz.timezone('US/Eastern')

    good_test_values = [('2014', 1388552400.0),
                        ('2014 ', 1388552400.0),
                        ('2014-02', 1391230800.0),
                        ('2014-02 ', 1391230800.0),
                        ('2014-2', 1391230800.0),
                        ('2014-2 ', 1391230800.0),
                        ('2014-2-10', 1392008400.0),
                        ('2014-2-10 ', 1392008400.0),
                        ('2014-02-10', 1392008400.0),
                        ('2014-02-10 ', 1392008400.0),
                        (' 2014-02-10 10 ', 1392044400.0),
                        ('2014-02-10 10:1', 1392044460.0),
                        ('2014-02-10 10:1 ', 1392044460.0),
                        ('2014-02-10 10:1:00', 1392044460.0),
                        ('2014-02-10 10:01:00', 1392044460.0),

                        # dst transistion tests
                        ('2015-03-08 01:59:59', 1425797999.0),  # is_dst==False
                        # at 2am, spring forward to 3am.
                        # [02:00:00 - 02:59:59] does not exist
                        ('2015-03-08 03:00:00', 1425798000.0),  # is_dst==True

                        ('2015-11-01 00:59:59', 1446353999.0),  # is_dst==True
                        # at 2am, fall back to 1am
                        # [01:00:00-01:59:59] is ambiguous without is_dst
                        ('2015-11-01 02:00:00', 1446361200.0),  # is_dst==False

                        # other
                        ttime.time(),
                        datetime.datetime.now(),
                        zone.localize(datetime.datetime.now()),
                        ]
    for val in good_test_values:
        yield _normalize_human_friendly_time_tester, val, True, None


    bad_test_values = ['2015-03-08 02:00:00',
                       '2015-03-08 02:59:59']
    for val in bad_test_values:
        yield _normalize_human_friendly_time_tester, val, False, pytz.NonExistentTimeError


    bad_test_values = ['2015-11-01 01:00:00',
                       '2015-11-01 01:59:59']
    for val in bad_test_values:
        yield _normalize_human_friendly_time_tester, val, False, pytz.AmbiguousTimeError


    bad_test_values = ['2015-04-15 03:',
                       str(ttime.time()),
                       'aardvark',
                       ]
    for val in bad_test_values:
        yield _normalize_human_friendly_time_tester, val, False, ValueError


# smoketests

def test_find_last_for_smoke():
    last, = mdsc.find_last()


if __name__ == "__main__":
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)
