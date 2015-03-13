from __future__ import print_function
import sys
import six  # noqa
import copy
from StringIO import StringIO
from collections import defaultdict, Iterable, deque
from .. import sources
from ..utils.console import color_print
from metadatastore.api import (Document, find_last, find_run_starts,
                               find_event_descriptors, find_run_stops,
                               find_events)
import warnings
from filestore.api import retrieve
import os
import logging


logger = logging.getLogger(__name__)


class DataBroker(object):

    @classmethod
    def __getitem__(cls, key):
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
            result = [Header.from_run_start(h) for h in result]
        elif isinstance(key, int):
            if key > -1:
                result = find_run_starts(scan_id=key)
                if len(result) == 0:
                    raise ValueError("No such run found.")
                result = result[0]  # most recent match
                result = Header.from_run_start(result)
            else:
                result = find_last(-key)
                if len(result) < -key:
                    raise IndexError(
                        "There are only {0} runs.".format(len(result)))
                result = result[-1]
                result = Header.from_run_start(result)
        elif isinstance(key, Iterable):
            return [cls.__getitem__(k) for k in key]
        else:
            raise ValueError("Must give an integer scan ID like [6], a slice "
                             "into past scans like [-5], [-5:], or [-5:-9:2], "
                             "or a list like [1, 7, 13].")
        return result

    @classmethod
    def fetch_events(cls, headers, ca_host=None, channels=None):
        """
        Get Events from given run(s).

        Parameters
        ----------
        headers : one RunHeader or a list of them
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
        try:
            headers.items()
        except AttributeError:
            pass
        else:
            headers = [headers]

        events = []
        for header in headers:
            descriptors = find_event_descriptors(
                    run_start_id=header.run_start_id)
            for descriptor in descriptors:
                events.extend(find_events(descriptor=descriptor))
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
        """Given search criteria, find Headers describing runs.

        This function returns a list of dictionary-like objects encapsulating
        the metadata for a run -- start time, instruments uses, and so on.
        In addition to the Parameters below, advanced users can specifiy
        arbitrary queries that are passed through to mongodb.

        Parameters
        ----------
        start_time : float, optional
            timestamp of the earliest time that a RunStart was created
        stop_time : float, optional
            timestamp of the latest time that a RunStart was created
        beamline_id : str, optional
            String identifier for a specific beamline
        project : str, optional
            Project name
        owner : str, optional
            The username of the logged-in user when the scan was performed
        scan_id : int, optional
            Integer scan identifier
        uid : str, optional
            Globally unique id string provided to metadatastore
        _id : str or ObjectId, optional
            The unique id generated by mongo

        Returns
        -------
        data : list
            Header objects

        Examples
        --------
        >>> find_headers(start_time=12345678)
        """
        run_start = find_run_starts(**kwargs)
        result = []
        for rs in run_start:
            result.append(Header.from_run_start(rs))
        return result


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


class EventQueue(object):
    """
    Get Events from Headers during data collection.

    This is a simple single-process implementation.

    Example
    -------

    from dataportal.broker import DataBroker, EventQueue
    header = DataBroker[-1]  # for example, most recent header
    queue = EventQueue(header)
    while True:
        queue.update()
        new_events = queue.get()
        # Do something with them, such as dm.append_events(new_events)
    """

    def __init__(self, headers):
        if hasattr(headers, 'keys'):
            # This is some kind of dict.
            headers = [headers]
        self.headers = headers
        self._known_ids = set()
        # This is nested, a deque of lists that are bundles of events
        # discovered in the same update.
        self._queue = deque()

    def update(self):
        """Obtain a fresh list of the relevant Events."""

        # like fetch_events, but we don't fill in the data right away
        events = []
        for header in self.headers:
            descriptors = find_event_descriptors(
                    run_start_id=header.run_start_id)
            for descriptor in descriptors:
                events.extend(find_events(descriptor=descriptor))
        if not events:
            return

        new_events = []
        for event in events:
            if event.id not in self._known_ids:
                new_events.append(event)
                self._known_ids.add(event.id)

        # The major performance savings is here: only fill the new events.
        [fill_event(event) for event in new_events]
        self._queue.append(new_events)  # the entry can be an empty list

    def get(self):
        """
        Get a list of new Events.

        Each call returns a (maybe empty) list of Events that were
        discovered in the same call to update().
        """
        # EventQueue is FIFO.
        try:
            return self._queue.popleft()
        except IndexError:
            return []


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
        if (data_key_dict and 'external' in data_key_dict):
            is_external[data_key] = bool(data_key_dict['external'])
    return is_external


def fill_event(event):
    """
    Populate events with externally stored data.
    """
    is_external = _inspect_descriptor(event.descriptor)
    for data_key, (value, timestamp) in event.data.items():
        if is_external[data_key]:
            # Retrieve a numpy array from filestore
            event.data[data_key][0] = retrieve(value)


class Header(Document):
    """A dictionary-like object summarizing metadata for a run."""

    @classmethod
    def from_run_start(cls, run_start):
        """
        Build a Header from a RunStart Document.

        Parameters
        ----------
        run_start : metadatastore.Document

        Returns
        -------
        header : dataportal.broker.Header
        """
        header = Header()
        header.event_descriptors = find_event_descriptors(run_start=run_start)
        run_stops = find_run_stops(run_start_uid=run_start.uid)
        try:
            run_stop, = run_stops
        except ValueError:
            num = len(run_stops)
            run_stop = None
            if num == 0:
                error_msg = ("A RunStop record could not be found for the "
                            "run with run_start_uid {0}".format(run_start.uid))
                warnings.warn(error_msg)
            else:
                error_msg = (
                    "{0} RunStop records (uids {1}) were found for the run with "
                    "run_start_uid {2}".format(num, [rs.uid for rs in run_stops],
                                            run_start.uid))
                if verify_integrity:
                    raise IntegrityError(error_msg)
                else:
                    warnings.warn(error_msg)

        # Map keys from RunStart and RunStop onto Header.
        attrs = {'start_time': 'time',
                'start_datetime': 'time_as_datetime',
                'scan_id': 'scan_id',
                'beamline_id': 'beamline_id',
                'owner': 'owner',
                'group': 'group',
                'project': 'project',
                'run_start_id': 'id',
                'run_start_uid': 'uid'}
        for new_key, old_key in attrs.items():
            header[new_key] = run_start[old_key]
        if run_stop is not None:
            attrs = {'stop_time': 'time',
                    'stop_datetime': 'time_as_datetime',
                    'exit_reason': 'reason',
                    'exit_status': 'exit_status',
                    'run_stop_id': 'id',
                    'run_stop_uid': 'uid'}
            for new_key, old_key in attrs.items():
                header[new_key] = run_stop[old_key]
        run_start._name = 'Header'
        return header

    def __repr__(self):
        # Even with a scan_id of 6+ digits, this fits under 80 chars.
        return "<Header scan_id={0} run_start_uid='{1}'>".format(
                self.scan_id, self.run_start_uid)

    def __str__(self):
        s = Stream()
        s.write("<Header>")
        s.write("Owner: {0}".format(self.owner))
        if self.group is not None:
            s.write("Group: {0}".format(self.group))
        if self.project is not None:
            s.write("Project: {0}".format(self.project))
        s.write("Project: {0}".format(self.project))
        s.write("Beamline ID: {0}".format(self.beamline_id), 'lightgray')
        s.write("Scan ID: {0} ".format(self.scan_id), 'green')
        s.write("Start Time: {0}".format(self.start_datetime), 'green')
        s.write("Stop Time: {0}".format(self.get('stop_datetime', 'Unknown')))
        s.write("Exit Status: {0}".format(self.get('exit_status', 'Unknown')))
        s.write("Exit Reason: {0}".format(self.get('exit_reason', 'Unknown')))
        for descriptor in self.event_descriptors:
            s.write("..Event Descriptor ", newline=False)
            s.write("(uid='{0}')".format(descriptor.uid), 'lightgrey')
            for data_key, data_key_dict in descriptor.data_keys.items():
                s.write("....", newline=False)
                s.write("{0}".format(data_key), 'green', newline=False)
                s.write(': {0}'.format(data_key_dict['source']),
                        color='lightgrey')
        s.write("run_start_uid='{0}'".format(self.run_start_uid),
                'lightgrey')
        if hasattr(self, 'run_stop_uid'):
            s.write("run_stop_uid='{0}'".format(self.run_stop_uid),
                    'lightgrey')
        return s.readout()


class Stream(object):

    def __init__(self):
        self._stringio = StringIO()
        self._stringio.isatty = sys.stdout.isatty

    def write(self, msg, color='default', newline=True):
        if newline:
            msg += '\n'
        color_print(msg, color, file=self._stringio)
        self._stringio.flush()

    def readout(self):
        self._stringio.seek(0)
        return self._stringio.read()


class IntegrityError(Exception):
    pass
