from __future__ import print_function
import six  # noqa
from collections import defaultdict, Iterable
from .. import sources
from .struct import BrokerStruct
import os
# Note: Invoke contents of sources at the func/method level so that it
# respects runtime switching between real and dummy sources.


# These should be specified elsewhere in a way that can be easily updated.
# This is merely a placeholder, but it can be used with the real
# channelarchiver as well as the dummy one.
POPULAR_CHANNELS = ['SR11BCM01:LIFETIME_MONITOR', 'SR11BCM01:CURRENT_MONITOR']
_SCARY_MONGOENGINE_METHODS = ['drop_collection', 'delete']


def get_events_by_run(runs, ca_host, channels=None):
    """
    Get Events from given run(s).

    Parameters
    ----------
    args : one or a list of BeginRun or EndRun objects
    ca_host : URL string
        the URL of your archiver's ArchiveDataServer.cgi. For example,
        'http://cr01arc01/cgi-bin/ArchiveDataServer.cgi'
    channels : list, optional
        All queries will return applicable data from the N most popular
        channels. If data from additional channels is needed, their full
        identifiers (not human-readable names) must be given here as a list
        of strings.

    Returns
    -------
    data : list of Event objects
    """
    find_event = sources.metadataStore.api.analysis.find_event

    # Normalize input: runs is a list of BeginRun and/or EndRun objects.
    if not isinstance(runs, Iterable):
        runs = [runs]

    events = [find_event(run) for run in runs]
    events = [e for run in events for e in run]  # flattened
    [_fill_event(event) for event in events]
    return events


def get_events_in_time_range(start_time, end_time, ca_host, channels=None):
    """
    Get Events that occurred between two times.

    Parameters
    ----------
    start_time : string or datetime object, optional
        e.g., datetime.datetime(2015, 1, 1) or '2015-01-01' (ISO format)
    end_time : string or datetime object, optional
        e.g., datetime.datetime(2015, 1, 1) or '2015-01-01' (ISO format)
    ca_host : URL string
        the URL of your archiver's ArchiveDataServer.cgi. For example,
        'http://cr01arc01/cgi-bin/ArchiveDataServer.cgi'
    channels : list, optional
        All queries will return applicable data from the N most popular
        channels. If data from additional channels is needed, their full
        identifiers (not human-readable names) must be given here as a list
        of strings.

    Returns
    -------
    data : list of Event objects
    """
    raise NotImplementedError()


def explore(**kwargs):
    """
    For now, pass through to metadataStore.api.analysis.find_header

    Parameters
    ----------
    **kwargs

    Returns
    -------
    data : list
        Header objects
    """
    find_header = sources.metadataStore.api.analysis.find_header
    return find_header(**kwargs)


def _assmeble_output(events):
    """Get data from external sources and channel archiver from mds events

    Convert event objects into list

    Parameters
    ----------
    events : list
        List of metadataStore.odm_templates.Event objects

    Returns
    -------

    """
    "Used by get_event_* functions"
    data = [_scrape_event(event) for event in events]

    archiver_data = get_archiver_data(ca_host, start_time, end_time)
    # Format data from the Archiver like data from the events, and combine.
    for ch_name, ch_data in zip(channels, archiver_result):
        data += [(time, {ch_name: value}) for time, value in
                 zip(ch_data.times, ch_data.values)]
    return data


def _get_archiver_data(ca_host, start_time, end_time, channels=None):
    # Get data from commonly-used Channel Archiver channels plus any
    # specified in the call.
    if channels is None:
        channels = []
    channels = list(set(channels) | set(POPULAR_CHANNELS))
    archiver = sources.channelarchiver.Archiver(ca_host)
    archiver_result = archiver.get(channels, start_time, end_time,
                                   interpolation='raw')  # never interpolate
    return archiver_result

class LocationError(ValueError):
    pass

def _get_local_cahost():
    """Obtain the url for the cahost by using the uname() function to
    grab the local beamline id

    References
    ----------
    https://github.com/NSLS-II/channelarchiver/README.rst
    """
    beamline_id = os.uname()[1][:4]
    if not beamline_id.startswith('xf'):
        raise LocationError('You are not on a registered beamline computer. '
                            'Unable to guess which channel archiver to use. '
                            'Please specify the channel archiver you wish to'
                            'obtain data from.')
    return 'http://' + beamline_id + '-ca/cgi-bin/ArchiveDataServer.cgi'

def get_last(channels=None, ca_host=None):
    mdsapi = sources.metadataStore.api.analysis
    bre = mdsapi.find_last()
    events = mdsapi.find_event(bre)
    print(events[0])
    # remove the foot cannons from the mongoengine objects
    bre = BrokerStruct(bre)
    events = [BrokerStruct(ev) for ev in events]
    # fill in the events from any external data sources
    [_fill_event(event) for event in events]
    tstart = bre.time
    tfinish = events[0].time
    if ca_host is None:
        try:
            ca_host = _get_local_cahost()
        except LocationError:
            sources.switch(channelarchiver=False)

    # archiver_data = _get_archiver_data(ca_host, tstart, tfinish, channels)

    return {'begin_run_event': bre,
            'events': events}

def _inspect_descriptor(descriptor):
    """
    Return a dict with the data keys mapped to boolean answering whether
    data is external.
    """
    # TODO memoize to cache these results
    data_keys = descriptor.data_keys
    is_external = defaultdict(lambda: False)
    for data_key, data_key_dict in data_keys.items():
        if 'external' in data_key_dict:
            is_external[data_key] = True
    return is_external


def _fill_event(event):
    """
    Populate events with externally stored data.
    """
    retrieve_data = sources.fileStore.commands.retrieve_data
    is_external = _inspect_descriptor(event.descriptor)
    for data_key, (value, timestamp) in event.data.items():
        if is_external[data_key]:
            # Retrieve a numpy array from filestore
            event.data[data_key] = retrieve_data(value)


def _scrape_event(event):
    """
    Return event.time, data where data is a dict of field names and values.

    Example
    -------
    >>> _parse_event(event)
    <UNIX EPOCH TIME>, {'chan1': <value>, 'chan2': <value>}
    """
    retrieve_data = sources.fileStore.commands.retrieve_data
    is_external = _inspect_descriptor(event.ev_desc)
    data = dict()
    for data_key, data_dict in event.data.items():
        value, _ = data_dict['value'], data_dict['timestamp']
        # Notice that the timestamp is not returned to the user, only the
        # event time, below.
        if is_external[data_key]:
            # Retrieve a numpy array from filestore
            data[data_key] = retrieve_data(value)
        else:
            # Store the scalar value, a Python primitive, directly.
            data[data_key] = value
    return event.time, data
