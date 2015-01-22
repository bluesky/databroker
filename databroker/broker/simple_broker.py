from __future__ import print_function
import six  # noqa
from collections import defaultdict
from .. import sources
# Note: Invoke contents of sources at the func/method level so that it
# respects runtime switching between real and dummy sources.


def search(beamline_id, start_time, end_time):
    "Get events from the MDS between these two times."
    find = sources.metadataStore.api.analysis.find
    events = find(start_time=start_time, end_time=end_time,
                  beamline_id=beamline_id)
    return [_parse_event(event) for event in events]


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
