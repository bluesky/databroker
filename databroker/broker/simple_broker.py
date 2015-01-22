from __future__ import print_function
import six  # noqa
from collections import defaultdict
from .. import sources
# Note: Invoke contents of sources at the func/method level so that it
# respects runtime switching between real and dummy sources.


# These should be specified elsewhere in a way that can be easily updated.
# This is merely a placeholder, but it can be used with the real
# channelarchiver as well as the dummy one.
POPULAR_CHANNELS = ['SR11BCM01:LIFETIME_MONITOR', 'SR11BCM01:CURRENT_MONITOR']


def search(beamline_id, start_time, end_time, ca_host, channels=None):
    """
    Get data from all events from a given beamline between two times.

    Parameters
    ----------
    beamline_id : string
        e.g., 'srx'
    start_time : string or datetime object
        e.g., datetime.datetime(2015, 1, 1) or '2015-01-01' (ISO format)
    end_time : string or datetime object
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
    data : list
        See example below illustrating the format of the returned dataset.

    Example
    -------
    >>> search('srx', '2015-01-01', '2015-01-02')
    [(<unix epoch time>, {'chan1': <value>, 'chan2': <value>},
     (<unix epoch time>, {'temp': <value>)}

    That is, it results a list of tuples, where each tuple contains a time and
    a dictionary of name/value pairs. Every value is guaranteed to be either a
    scalar Python primitive (int, float, string) or a numpy ndarray.
    """
    # Get data and populate external references from the File Store.
    find = sources.metadataStore.api.analysis.find
    events = find(start_time=start_time, end_time=end_time,
                  beamline_id=beamline_id)
    data = [_parse_event(event) for event in events]

    # Get data from commonly-used Channel Archiver channels plus any
    # specified in the call.
    if channels is None:
        channels = []
    channels = list(set(channels) | set(POPULAR_CHANNELS))
    archiver = sources.channelarchiver.Archiver(ca_host)
    archiver_result = archiver.get(channels, start_time, end_time,
                                   interpolation='raw')  # never interpolate

    # Format data from the Archiver like data from the events, and combine.
    for ch_name, ch_data in zip(channels, archiver_result):
        data += [(time, {ch_name: value}) for time, value in
                 zip(ch_data.times, ch_data.values)]
    return data


def _inspect_descriptor(descriptor):
    # TODO memoize to cache these results
    data_keys = descriptor.keys
    is_external = defaultdict(lambda: False)
    for data_key, data_key_dict in data_keys.items():
        if 'external' in data_key_dict.keys():
            is_external[data_key] = True
    return is_external


def _parse_event(event):
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
