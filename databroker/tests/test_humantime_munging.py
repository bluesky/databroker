from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time as ttime
import datetime

import pytz
import pytest
from databroker.headersource import mongo_core as core


# ### Testing metadatastore find functionality ################################
def _make_fint_func_dectorator():
    test_dict = {
        'find_run_starts': [
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
            {'uid': 'run_start_uid'}],
        'find_run_stops': [
            {'start_time': ttime.time()},
            {'stop_time': ttime.time()},
            {'start_time': ttime.time()-1, 'stop_time': ttime.time()},
            {'reason': 'whimsy'},
            {'exit_status': 'success'},
            {'run_start': 'rs'},
            {'run_start_uid': 'rs.uid'},
            {'uid': 'foo'}],
        'find_descriptors': [
            {'run_start': 'rs'},
            {'run_start': 'rs.uid'},
            {'start_time': ttime.time()},
            {'stop_time': ttime.time()},
            {'start_time': ttime.time() - 1, 'stop_time': ttime.time()},
            {'uid': 'foo'}],
    }
    targets = []
    for func, list_o_dicts in test_dict.items():
        for dct in list_o_dicts:
            targets.append([func, dct])

    return pytest.mark.parametrize('func,kw', targets)


# ### Test metadatastore time formatting ######################################

def _make_time_params():
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

    rets = []
    for val in good_test_values:
        rets.append([val, True, None])

    bad_test_values = ['2015-03-08 02:00:00',
                       '2015-03-08 02:59:59']
    for val in bad_test_values:
        rets.append([val, False, pytz.NonExistentTimeError])

    bad_test_values = ['2015-11-01 01:00:00',
                       '2015-11-01 01:59:59']
    for val in bad_test_values:
        rets.append([val, False, pytz.AmbiguousTimeError])

    bad_test_values = ['2015-04-15 03:',
                       str(ttime.time()),
                       'aardvark',
                       ]
    for val in bad_test_values:
        rets.append([val, False, ValueError])

    return pytest.mark.parametrize('val,should_succeed,etype', rets)


@_make_time_params()
def test_normalize_human_friendly_time(val, should_succeed, etype):
    if isinstance(val, tuple):
        (val, check_output) = val

    if should_succeed:
        output = core._normalize_human_friendly_time(val, 'US/Eastern')
        assert(isinstance(output, float))
        try:
            assert output == check_output
        except NameError:
            pass
    else:
        with pytest.raises(etype):
            core._normalize_human_friendly_time(val, 'US/Eastern')
