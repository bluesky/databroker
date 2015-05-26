from __future__ import print_function
import sys
import six  # noqa
from six import StringIO
from collections import defaultdict, Iterable, deque
from ..utils.console import color_print
from metadatastore.api import (Document, find_last, find_run_starts,
                               find_event_descriptors, find_run_stops,
                               find_events)
from bson import ObjectId
import warnings
from filestore.api import retrieve
import os
import logging


logger = logging.getLogger(__name__)


class _DataBrokerClass(object):
    # A singleton is instantiated in broker/__init__.py.
    # You probably do not want to instantiate this; use
    # broker.DataBroker instead.

    @classmethod
    def __getitem__(cls, key):
        if isinstance(key, slice):
            # Interpret key as a slice into previous scans.
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
            result = list(find_last(start))[stop::key.step]
            header = [Header.from_run_start(h) for h in result]
        elif isinstance(key, int):
            if key > -1:
                # Interpret key as a scan_id.
                gen = find_run_starts(scan_id=key)
                try:
                    result = next(gen)  # most recent match
                except StopIteration:
                    raise ValueError("No such run found.")
                header = Header.from_run_start(result)
            else:
                # Interpret key as the Nth last scan.
                gen = find_last(-key)
                for i in range(-key):
                    try:
                        result = next(gen)
                    except StopIteration:
                        raise IndexError(
                            "There are only {0} runs.".format(i))
                header = Header.from_run_start(result)
        elif isinstance(key, six.string_types):
            # Interpret key as a uid (or the few several characters of one).
            # First try searching as if we have the full uid.
            results = list(find_run_starts(uid=key))
            if len(results) == 0:
                # No dice? Try searching as if we have a partial uid.
                gen = find_run_starts(uid={'$regex': '{0}.*'.format(key)})
                results = list(gen)
            if len(results) < 1:
                raise ValueError("No such run found.")
            if len(results) > 1:
                raise ValueError("That partial uid matches multiple runs. "
                                 "Provide more characters.")
            result, = results
            header = Header.from_run_start(result)
        elif isinstance(key, Iterable):
            # Interpret key as a list of several keys. If it is a string
            # we will never get this far.
            return [cls.__getitem__(k) for k in key]
        else:
            raise ValueError("Must give an integer scan ID like [6], a slice "
                             "into past scans like [-5], [-5:], or [-5:-9:2], "
                             "a list like [1, 7, 13], or a (partial) uid "
                             "like ['a23jslk'].")
        return header

    @classmethod
    def fetch_events(cls, headers, fill=True):
        """
        Get Events from given run(s).

        Parameters
        ----------
        headers : RunHeader or iterable of RunHeader
            The headers to fetch the events for

        fill : bool, optional
            If non-scalar data should be filled in, Defaults to True

        Yields
        ------
        event : Event
            The event, optionally with non-scalar data filled in
        """
        try:
            headers.items()
        except AttributeError:
            pass
        else:
            headers = [headers]

        for header in headers:
            descriptors = find_event_descriptors(
                    run_start_id=header.run_start_id)
            for descriptor in descriptors:
                for event in find_events(descriptor=descriptor):
                    if fill:
                        fill_event(event)
                    yield event

    @classmethod
    def find_headers(cls, **kwargs):
        """Given search criteria, find Headers describing runs.

        This function returns a list of dictionary-like objects encapsulating
        the metadata for a run -- start time, instruments uses, and so on.
        In addition to the Parameters below, advanced users can specifiy
        arbitrary queries that are passed through to mongodb.

        Parameters
        ----------
        start_time : time-like, optional
            Include Headers for runs started after this time. Valid
            "time-like" representations are:
                - float timestamps (seconds since 1970), such as time.time()
                - '2015'
                - '2015-01'
                - '2015-01-30'
                - '2015-03-30 03:00:00'
                - Python datetime objects, such as datetime.datetime.now()
        stop_time: time-like, optional
            Include Headers for runs started before this time. See
            `start_time` above for examples.
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
        data_key : str, optional
            The alias (e.g., 'motor1') or PV identifier of data source

        Returns
        -------
        data : list
            Header objects

        Examples
        --------
        >>> find_headers(start_time='2015-03-05', stop_time='2015-03-10')
        >>> find_headers(data_key='motor1')
        >>> find_headers(data_key='motor1', start_time='2015-03-05')
        """
        data_key = kwargs.pop('data_key', None)
        run_start = find_run_starts(**kwargs)
        if data_key is not None:
            node_name = 'data_keys.{0}'.format(data_key)
            query = {node_name: {'$exists': True},
                     'run_start_id': {'$in': [ObjectId(rs.id) for rs in run_start]}}
            descriptors = find_event_descriptors(**query)
            result = []
            known_ids = deque()
            for descriptor in descriptors:
                if descriptor.run_start.id not in known_ids:
                    rs = descriptor.run_start
                    known_ids.append(rs.id)
                    result.append(rs)
            run_start = result
        result = []
        for rs in run_start:
            result.append(Header.from_run_start(rs))
        return result


class EventQueue(object):
    """
    Get Events from Headers during data collection.

    This is a simple single-process implementation.

    Example
    -------

    >>> from dataportal.broker import DataBroker, EventQueue
    >>> header = DataBroker[-1]  # for example, most recent header
    >>> queue = EventQueue(header)
    >>> while True:
    ...    queue.update()
    ...    new_events = queue.get()
    ...    # Do something with them, such as dm.append_events(new_events)
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
                events.extend(list(find_events(descriptor=descriptor)))
        if not events:
            return

        new_events = []
        for event in events:
            if event.uid not in self._known_ids:
                new_events.append(event)
                self._known_ids.add(event.uid)

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
    for data_key, value in six.iteritems(event.data):
        if is_external[data_key]:
            # Retrieve a numpy array from filestore
            event.data[data_key] = retrieve(value)


class Header(Document):
    """A dictionary-like object summarizing metadata for a run."""

    @classmethod
    def from_run_start(cls, run_start, verify_integrity=True):
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
        header._name = "Header"
        header.event_descriptors = list(
            find_event_descriptors(run_start=run_start))
        run_stops = list(find_run_stops(run_start_id=run_start.id))
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
                    "{0} RunStop records (uids {1}) were found for the run "
                    "with run_start_uid {2}".format(
                        num, [rs.uid for rs in run_stops], run_start.uid))
                if verify_integrity:
                    raise IntegrityError(error_msg)
                else:
                    warnings.warn(error_msg)

        # Map keys from RunStart and RunStop onto Header.
        run_start_renames = {'start_time': 'time',
                             'start_datetime': 'time_as_datetime',
                             'scan_id': 'scan_id',
                             'beamline_id': 'beamline_id',
                             'owner': 'owner',
                             'group': 'group',
                             'project': 'project',
                             'run_start_id': 'id',
                             'run_start_uid': 'uid'}

        run_start_renames_back = {v: k for k, v
                                  in run_start_renames.items()}
        for k in run_start:
            new_key = run_start_renames_back.get(k, k)
            header[new_key] = run_start[k]

        if run_stop is not None:
            run_stop_renames = {'stop_time': 'time',
                                'stop_datetime': 'time_as_datetime',
                                'exit_reason': 'reason',
                                'exit_status': 'exit_status',
                                'run_stop_id': 'id',
                                'run_stop_uid': 'uid'}
            run_stop_renames_back = {v: k for k, v
                                     in run_stop_renames.items()}
            for k in run_stop:
                new_key = run_stop_renames_back.get(k, k)
                header[new_key] = run_stop[k]

        run_start._name = 'Header'
        return header

    def __repr__(self):
        # Even with a scan_id of 6+ digits, this fits under 80 chars.
        return "<Header scan_id={0} run_start_uid='{1}'>".format(
            self.scan_id, self.run_start_uid)

    @property
    def summary(self):
        special_keys = set(('owner', 'group', 'project', 'beamline_id',
                            'scan_id', 'start_datetime', 'stop_datetime',
                            'exit_status', 'exit_reason', 'run_start_uid',
                            'run_stop_uid', ))
        s = Stream()
        s.write("<Header>")
        s.write("Owner: {0}".format(self.owner))
        if self.group:
            s.write("Group: {0}".format(self.group))
        if self.project:
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
        for k in sorted(self.keys()):
            if k in special_keys:
                continue
            s.write("{0}: {1}".format(k, self[k]))

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
