__author__ = 'arkilic'
from metadataStore.api.collection import search
from metadataStore.api.collection import create_header as _create_header
from metadataStore.api.collection import (create_event_descriptor
                                          as _create_event_descriptor)

import epics
import json
import six
import sys
import time

header_PV = "XF:23ID-CT{Replay}Val:RunHdr-I"
event_PV = "XF:23ID-CT{Replay}Val:EventHdr-I"


def sleep(sleep_time, sleep_msg):
    """
    Sleep the thread for `sleep_time` seconds
    Parameters
    ----------
    sleep_time : float
        Number of seconds to sleep the thread
    sleep_mst : str, optional
        String to print to the console along with a sleep timer
    """
    if sleep_msg is None:
        sleep_msg = ""
    print('Sleeping for {} ms. {}'.format(str(sleep_time*1000), sleep_msg))
    time.sleep(sleep_time)


def create_event_descriptor(run_header, event_type_id, data_keys,
                            descriptor_name, type_descriptor=None):
    """Create an event descriptor

    Parameters
    ----------
    run_header : dict
        Run header formatted by `create_run_header` that this event_descriptor
        is supposed to belongt o
    event_type_id : int
        The event type id of the event descriptor
    data_keys : list
        The names of the data keys that the scan cares about
    descriptor_name : string
        Textual description of this event_descriptor
    type_descriptor : dict, optional
        Information bucket for the event descriptor. Should probably hold at
        least a mapping from PV aliases to PV names
    """
    print('event descriptor data keys: {}'.format(data_keys))
    if type_descriptor is None:
        type_descriptor = {}
    scan_id = run_header['scan_id']
    if six.PY2:
        to_str = str
    elif six.PY3:
        to_str = six.text_type
    data_keys = [to_str(key) for key in data_keys]
    event_descriptor = {'scan_id': scan_id,
                        'event_type_id': int(event_type_id),
                        'data_keys': data_keys,
                        'descriptor_name': descriptor_name,
                        'type_descriptor': type_descriptor}
    while not search(scan_id=scan_id):
        res = search(scan_id=scan_id)
        sleep(0.5, "Header for scan_id {} not found. Give it a few seconds "
                   "before terminating".format(scan_id))

    # create the event descriptor
    _create_event_descriptor(**event_descriptor)

    databroker_header = _get_header(scan_id=scan_id)
    found = False
    while not found:
        search_results = search(scan_id=scan_id)
        if not search_results:
            sleep(0.5, "Event descriptor for scan_id {} not found. Give it "
                       "a few seconds before terminating".format(scan_id))
        else:
            found = True
            event_descriptor = search_results['event_descriptors']
            for idx, (ev_desc_id, ev_desc) in enumerate(
                    six.iteritems(event_descriptor)):
                print('idx: {}\n\tid: {}\n\t contents: {}\n'.format(
                    idx, ev_desc_id, ev_desc))
            print('event_descriptor: {}'.format(event_descriptor))
            # make sure that i only found one header
            if len(list(six.iterkeys(event_descriptor))) > 1:
                raise ValueError("Searching for multiple event descriptors is "
                                 "not implemented yet...")
            # grab the first and only header
            event_descriptor = list(six.itervalues(event_descriptor))[0]
    # get rid of the ObjectID thing that can't be JSON serialized
    event_descriptor['_id'] = str(event_descriptor['_id'])
    event_descriptor['header_id'] = str(event_descriptor['header_id'])
    print('event_descriptor: {}'.format(event_descriptor))
    return event_descriptor

def _get_header(scan_id):
    found = False
    while not found:
        databroker_header = search(scan_id=scan_id)
        if not databroker_header:
            sleep(0.5, "Header for scan_id {} not found. Give it a few seconds "
                       "before terminating".format(scan_id))
        else:
            found = True
            databroker_header = databroker_header['headers']
            # make sure that i only found one header
            if len(list(six.iterkeys(databroker_header))) > 1:
                raise ValueError("Something has gone seriously sideways. Multiple data "
                                 "broker headers were found and __only__ one should "
                                 "be...")
            # grab the first and only header
            databroker_header = list(six.itervalues(databroker_header))[0]

    return databroker_header


def create_run_header(scan_id, scan_name=None, owner=None, status=None,
                      tags=None, force=True, start_time=None, custom=None,
                      **kwargs):
    """
    Parameters
    ----------
    scan_id : int
        The preferred scan id. If 'force' is True, then increment the scan_id
        until a valid scan_id is found. Otherwise a ValueError will be raised.
    scan_name : str, optional
        The name of the scan
    owner : str, optional
        The name of the operator of the scan. Defaults to login id
    status : {'In Progress', 'Complete'}, optional
        The current status of the run, defaults to 'In Progress'
    tags : list, optional
        The tags to apply to this scan
    force : bool, optiona;
        Force creation of the run header by indexing the scan_id until a valid
        id is found.

    Returns
    -------
    header : dict
        Properly formatted run header that exists in the metadataStore

    Raises
    ------
    ValueError
        A ValueError is raised if `scan_id` is already taken and `force`=False
    """
    header = {'scan_id': scan_id, 'custom': {}}
    if owner is not None:
        header['owner'] = owner
    if tags is not None:
        header['tags'] = tags
    if status is not None:
        header['status'] = status
    if start_time is not None:
        header['start_time'] = start_time
    if custom is not None:
        header['custom'] = custom
    if scan_name is not None:
        header['custom'].update({'scan_name': scan_name})
    # dump the kwargs into the custom field of the run header
    if kwargs:
        header['custom'].update(kwargs)

    # create the header
    created = False
    # this while loop is needed to force creation of the run header when
    # 'scan_id' collisions occur
    while not created:
        try:
            _create_header(**header)
            created = True
        except KeyError as ke:
            if force:
                header['scan_id'] += 1
                sleep_msg = ('Searching for valid scan number. Current '
                             'index: {}'.format(header['scan_id']))
                sleep(0.1, sleep_msg)
            else:
                six.reraise(KeyError, KeyError(ke), sys.exc_info()[2])
    scan_id = header['scan_id']
    databroker_header = _get_header(scan_id=scan_id)
    # get rid of the ObjectID thing that can't be JSON serialized
    databroker_header['_id'] = str(databroker_header['_id'])

    # print('databroker_runheader: {}'.format(databroker_header))
    return databroker_header


def format_event(header, event_descriptor, seq_no, data):
    """
    For valid keys in kwargs, see 'event_keys_dict'
    """
    scan_id = header['scan_id']
    owner = header['owner']
    # before going to the trouble of formatting an event, make sure that the
    # header exists
    res = search(scan_id = scan_id)
    if not res:
        raise Exception('Header with given scan_id is not yet created. '
                        'Something has gone seriously sideways. Current state'
                        'of this function call...'
                        '\nheader: {}'
                        '\nevent_descriptor: {}'
                        '\ndescription: {}'
                        '\nseq_no: {}'
                        '\ndata: {}'.format(header, event_descriptor, seq_no,
                                            data))
    # create the event dict
    event = {'seq_no': seq_no,
             'data': data,
             'owner': owner,
             'scan_id': scan_id,
             'descriptor_name': event_descriptor['descriptor_name'],
             'descriptor_id': event_descriptor['_id'],
             'header_id': header['_id'],
             }

    return event


def write_to_hdr_PV(header, event_descriptor):
    packaged = dict()
    header_pv = epics.PV(header_PV)
    try:
        packaged['header'] = header
        packaged['event_descriptors'] = [event_descriptor, ]
        #_create(header=header)
        jsonified = json.dumps(packaged)
        print 'Writing to PV', jsonified
        header_pv.put(jsonified)
    except:
        raise


def write_to_event_PV(event):
    event_pv = epics.PV(event_PV)
    try:
        #_record(event=event)
        dumped_ev = json.dumps(event)
        event_pv.put(dumped_ev)
    except:
        raise
