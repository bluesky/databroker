from __future__ import print_function
import six  # noqa
import copy
from collections import defaultdict, Iterable, deque
from .. import sources
from metadatastore.api import Document
import os
# Note: Invoke contents of sources at the func/method level so that it
# respects runtime switching between real and dummy sources.


# These should be specified elsewhere in a way that can be easily updated.
# This is merely a placeholder, but it can be used with the real
# channelarchiver as well as the dummy one.


class DataBroker(object):

    @classmethod
    def __getitem__(cls, key):
        # Define imports here so that sources can be switched
        # at run time.
        find_last = sources.metadatastore.api.find_last
        find_run_start = sources.metadatastore.api.find_run_start
        if isinstance(key, slice):
            # Slice on recent runs.
            if key.start is not None and key.start > -1:
                raise ValueError("Slices must be negative. The most recent "
                                 "run is referred to as -1.")
            if key.stop is not None and key.stop > -1:
                raise ValueError("Slices must be negative. The most recent "
                                 "run is referred to as -1.")
            if key.stop is not None:
                stop = -key.stop
            else:
                stop = None
            if key.start is None:
                raise ValueError("Cannot slice infinitely into the past; "
                                 "the result could become too large.")
            start = -key.start
            result = find_last(start)[stop::key.step]
            [_build_header(h) for h in result]
        elif isinstance(key, int):
            if key > -1:
                result = find_run_start(scan_id=key)
                if len(result) == 0:
                    raise ValueError("No such run found.")
                result = result[0]  # most recent match
                _build_header(result)
            else:
                result = find_last(-key)
                if len(result) < -key:
                    raise IndexError(
                        "There are only {0} runs.".format(len(result)))
                result = result[-1]
                _build_header(result)
        else:
            raise ValueError("Must give an integer scan ID like [6], a slice "
                             "into past scans like [-5], [-5:], or [-5:-9:2], "
                             "or a list like [1, 7, 13].")
        return result

    @classmethod
    def fetch_events(cls, runs, ca_host=None, channels=None):
        """
        Get Events from given run(s).

        Parameters
        ----------
        runs : one RunHeader or a list of them
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
        data : a flat list of Event objects
        """
        find_event = sources.metadatastore.api.find_event

        if not isinstance(runs, Iterable):
            runs = [runs]

        events_by_descriptor = []
        for run in runs:
            run = copy.copy(run)
            run.id = run.ids['run_start_id']
            events_by_descriptor.extend(find_event(run))
        events = [event for descriptor in events_by_descriptor
                  for event in descriptor]
        [fill_event(event) for event in events]

        if channels is not None:
            if ca_host is None:
                ca_host = _get_local_ca_host()
            all_times = [event.time for event in events]
            archiver_data = _get_archiver_data(ca_host, channels,
                                               min(all_times), max(all_times))
        return events

    @classmethod
    def find_headers(cls, **kwargs):
        """
        For now, pass through to metadatastore.api.analysis.find_header

        Parameters
        ----------
        **kwargs

        Returns
        -------
        data : list
            Header objects
        """
        find_run_start = sources.metadatastore.api.find_run_start
        run_start = find_run_start(**kwargs)
        for rs in run_start:
            _build_header(rs)
        return run_start  # these have been built out into headers


def _get_archiver_data(ca_host, channels, start_time, end_time):
    archiver = sources.channelarchiver.Archiver(ca_host)
    archiver_result = archiver.get(channels, start_time, end_time,
                                   interpolation='raw')  # never interpolate
    # Put archiver data into Documents, minimicking a MDS event stream.
    events = list()
    for ch_name, ch_data in zip(channels, archiver_result):
        # Build a Event Descriptor.
        descriptor = dict()
        descriptor.time = start_time
        descriptor['data_keys'] = {ch_name: dict(source=ch_name,
                                   shape=[], dtype='number')}
        descriptor = Document(descriptor)
        for time, value in zip(ch_data.times, ch_data, values):
            # Build an Event.
            event = dict()
            event['descriptor'] = descriptor
            event['time'] = time
            event['data'] = {ch_name: (value, time)}
            event = Document(event)
            events.append(event)
    return events


class LocationError(ValueError):
    pass


def _get_local_ca_host():
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


def fill_event(event):
    """
    Populate events with externally stored data.
    """
    retrieve_data = sources.filestore.api.retrieve
    is_external = _inspect_descriptor(event.descriptor)
    for data_key, (value, timestamp) in event.data.items():
        if is_external[data_key]:
            # Retrieve a numpy array from filestore
            event.data[data_key][0] = retrieve_data(value)


def _build_header(run_start):
    "Modify a RunStart Document in place into a Header Document."
    mdsapi = sources.metadatastore.api
    run_start.event_descriptors = mdsapi.find_event_descriptor(run_start)
    run_stop = mdsapi.find_run_stop(run_start)
    # fix the time issue
    adds = {'start_time': (run_start, 'time'),
            'start_datetime': (run_start, 'time_as_datetime')}
    deletes = [(run_start, '_name')]
    add_to_id = {'run_start_uid': (run_start, 'uid'),
                 'run_start_id': (run_start, 'id')}
    if run_stop is not None:
        adds['stop_time'] = (run_stop, 'time')
        adds['stop_datetime'] = (run_stop, 'time_as_datetime')
        adds['exit_reason'] = (run_stop, 'reason')
        deletes.append((run_stop, '_name'))
        deletes.append((run_stop, 'run_start'))
        add_to_id['run_stop_uid'] = (run_stop, 'uid')
        add_to_id['run_stop_id'] = (run_stop, 'id')
    for new_var_name, src_tuple in adds.items():
        setattr(run_start, new_var_name, getattr(*src_tuple))
        delattr(*src_tuple)
    for to_delete in deletes:
        delattr(*to_delete)
    run_start.ids = {}
    for new_var_name, src_tuple in add_to_id.items():
        run_start.ids[new_var_name] = getattr(*src_tuple)
        delattr(*src_tuple)
    if run_stop is not None:
       # dump the remaining values from the RunStop object into the header
        for k, v in vars(run_stop).items():
            if hasattr(run_start, k):
                raise ValueError("The run header already has a key named {}. "
                                 "Please update the mappings".format(k))
            setattr(run_start, k, v)

    run_start._name = 'Header'
