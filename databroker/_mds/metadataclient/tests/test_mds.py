import time as ttime
import uuid
import pytest
from ....headersource.mongo_core import NoEventDescriptors, NoRunStart

# if these are not commented out, the skip propagates up to base
from ....tests.test_mds import (
    test_bad_bulk_insert_event_data,
    test_bad_bulk_insert_event_timestamp,
    test_bad_event_desc,
    test_bulk_insert,
    test_bulk_table,
#    test_cache_clear_lookups,
    test_iterative_insert,
    test_custom_warn,
    test_double_run_stop,
    test_event_descriptor_insertion,
    test_fail_runstart,
    test_find_run_start,
    test_find_run_stop,
    test_insert_run_start,
    test_no_evdesc,
    test_pickle,
    test_run_stop_by_run_start,
    test_run_stop_insertion)



def test_no_evdesc(mds_all):
    mdsc = mds_all
    run_start_uid = mdsc.insert_run_start(
        scan_id=42, beamline_id='testbed', owner='tester',
        group='awesome-devs', project='Nikea', time=ttime.time(),
        uid=str(uuid.uuid4()))

    with pytest.raises(NoEventDescriptors):
        mdsc.descriptors_by_start(run_start_uid)


def test_fail_runstart(mds_all):
    mdsc = mds_all
    with pytest.raises(NoRunStart):
        mdsc.run_start_given_uid('aardvark')
