from __future__ import print_function
import sys
import humanize
import time as ttime
import six  # noqa
from six import StringIO
from collections import Iterable, deque
from ..utils.console import color_print
from metadatastore.api import (find_last, find_run_starts,
                               find_descriptors,
                               find_events)

import metadatastore.doc as doc
from metadatastore.doc import pretty_print_time
import metadatastore.commands as mc

import filestore.api as fs
import os
import logging
import uuid


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
            header = Headers([Header.from_run_start(h) for h in result])
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
            return Headers([cls.__getitem__(k) for k in key])
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
            descriptors = find_descriptors(
                run_start=header['uid'])
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

            query = {node_name: {'$exists': True}}
            descriptors = []
            for rs in run_start:
                descriptor = find_descriptors(run_start=rs, **query)
                for d in descriptor:
                    descriptors.append(d)
            # query = {node_name: {'$exists': True},
            #          'run_start_id': {'$in': [ObjectId(rs.id) for rs in run_start]}}
            # descriptors = find_descriptors(**query)
            result = []
            known_uids = deque()
            for descriptor in descriptors:
                if descriptor.run_start.uid not in known_uids:
                    rs = descriptor.run_start
                    known_uids.append(rs.uid)
                    result.append(rs)
            run_start = result
        result = []
        for rs in run_start:
            result.append(Header.from_run_start(rs))
        return Headers(result)


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
        self._known_uids = set()
        # This is nested, a deque of lists that are bundles of events
        # discovered in the same update.
        self._queue = deque()

    def update(self):
        """Obtain a fresh list of the relevant Events."""

        # like fetch_events, but we don't fill in the data right away
        events = []
        for header in self.headers:
            descriptors = find_descriptors(run_start=header['uid'])
            for descriptor in descriptors:
                events.extend(list(find_events(descriptor=descriptor)))
        if not events:
            return

        new_events = []
        for event in events:
            if event.uid not in self._known_uids:
                new_events.append(event)
                self._known_uids.add(event.uid)

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
    is_external = dict()
    for data_key, data_key_dict in data_keys.items():
        is_external[data_key] = data_key_dict.get('external', False)
    return is_external


def fill_event(event):
    """
    Populate events with externally stored data.
    """
    is_external = _inspect_descriptor(event.descriptor)
    for data_key, value in six.iteritems(event.data):
        if is_external[data_key]:
            # Retrieve a numpy array from filestore
            event.data[data_key] = fs.retrieve(value)


class Header(doc.Document):
    """A dictionary-like object summarizing metadata for a run."""

    @classmethod
    def from_run_start(cls, run_start, verify_integrity=False):
        """
        Build a Header from a RunStart Document.

        Parameters
        ----------
        run_start : metadatastore.document.Document

        Returns
        -------
        header : dataportal.broker.Header
        """
        return make_header(run_start, not verify_integrity)

    def __repr__(self):
        # Even with a scan_id of 6+ digits, this fits under 80 chars.
        return "<Header scan_id={0} run_start_uid={1!r}>".format(
            self['scan_id'], self['uid'])

    @property
    def summary(self):
        return summerize_header(self)


def make_header(run_start, allow_no_runstop=False):
    header = dict()
    # make sure that our runstart is really a document
    # and get the uid
    run_start_uid = mc.doc_or_uid_to_uid(run_start)
    run_start = mc.runstart_given_uid(run_start_uid)
    # fill in the runstart
    header['run_start'] = run_start

    # fields to copy out of runstart into top-level header
    run_start_copy = {'start_time': 'time',
                      'time': 'time',
                      'scan_id': 'scan_id',
                      'uid': 'uid',
                      'sample': 'sample'}

    for h_key, rs_key in run_start_copy.items():
        header[h_key] = run_start[rs_key]

    # fields to copy to top-level header
    run_stop_copy = {'stop_time': 'time',
                     'exit_reason': 'reason',
                     'exit_status': 'exit_status'}

    # see if we have a runstop, ok if we don't
    try:
        run_stop = mc.runstop_by_runstart(run_start_uid)
        for h_key, rs_key in run_stop_copy.items():
            if rs_key == 'reason':
                header[h_key] = run_stop.get(rs_key, '')
            else:
                header[h_key] = run_stop[rs_key]

        header['run_stop'] = doc.ref_doc_to_uid(run_stop, 'run_start')

    except mc.NoRunStop:
        if allow_no_runstop:
            header['run_stop'] = None
            for k in run_stop_copy:
                header[k] = None
        else:
            raise

    try:
        ev_descs = [doc.ref_doc_to_uid(ev_desc, 'run_start')
                    for ev_desc in
                    mc.descriptors_by_runstart(run_start_uid)]

    except mc.NoEventDescriptors:
        ev_descs = []

    header['descriptors'] = ev_descs
    return doc.Document('header', header)


def summerize_header(header):
    special_keys = set(('start_time', 'time', 'stop_time', 'scan_id',
                        'uid', 'descriptors', 'sample', 'exit_status',
                        'exit_reason', 'event_descriptors'))
    run_start = header['run_start']
    s = Stream()
    s.write("<Header ", newline=False)
    s.write("#{0} ".format(header['scan_id']), 'green', newline=False)
    s.write("{0!r}".format(header['uid'][:6]), color='red', newline=False)
    s.write(">")

    s.write("Sample: {0!r} ".format(run_start['sample']))
    s.write("Start Time: {0}".format(pretty_print_time(header['start_time'])))
    if header['run_stop'] is None:
        st_tm, exit_status, exit_reason = ['Unknown'] * 3
    else:
        st_tm = pretty_print_time(header['stop_time'])
        exit_status = header['exit_status']
        exit_reason = header['exit_reason']

    s.write("Stop Time: {0}".format(st_tm))
    if header['stop_time']:
        dur = humanize.naturaldelta(header['stop_time'] - header['start_time'])
        s.write('Run Duration : {0}'.format(dur))
    s.write("Exit Status: {0}".format(exit_status))
    s.write("Exit Reason: {0}".format(exit_reason))
    s.write("uid={0!r}".format(header['uid']), 'lightgrey')
    s.write("Event Descriptors:")
    for descriptor in header['descriptors']:
        s.write("..Descriptor ", newline=False)
        s.write("({0!r})".format(descriptor['uid']), 'lightgrey')
        for data_key, data_key_dict in descriptor.data_keys.items():
            s.write("....", newline=False)
            s.write("{0}".format(data_key), 'green', newline=False)
            s.write(': {0}'.format(data_key_dict['source']),
                    color='lightgrey', newline=False)
            s.write(" <{0}".format(data_key_dict['dtype']), newline=False)
            shape = data_key_dict['shape']
            if shape:
                s.write(', {}'.format(shape), newline=False)
            s.write('>')
    for k in sorted(header):
        if k in special_keys:
            continue
        elif k in ('run_stop', 'run_start'):
            s.write("{0}:".format(k))
            for _k, v in sorted(header[k].items()):
                if _k in 'run_start':
                    v = repr(v['uid'])
                elif _k == 'uid':
                    v = repr(v)
                elif _k == 'time':
                    v = pretty_print_time(v)
                s.write("..{0}: {1}".format(_k, v))
        else:
            s.write("{0}: {1}".format(k, header[k]))

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


class Headers(list):
    """Put a nice HTML repr on list"""
    # See http://getbootstrap.com/javascript/#collapse-example-accordion

    def _repr_html_(self):
        repr_uid = uuid.uuid4()
        prefix = """
<div class="panel-group" id="accordian-{repr_uid}" role="tablist" aria-multiselectable="true">""".format(repr_uid=repr_uid)
        element = """
  <div class="panel panel-default">
    <div class="panel-heading" role="tab" id="heading-{uid}" style="text-overflow: ellipsis; white-space: nowrap; overflow: hidden;">
      <h4 class="panel-title">
        <a data-toggle="collapse" data-parent="#accordion-{repr_uid}" href="#collapse-{uid}" aria-expanded="false" aria-controls="collapse-{uid}">
          Header: Scan {header.scan_id} &nbsp; <span style="color: #AAAAAA;">{human_time}</span>
        </a>
      </h4>
    </div>
    <div id="collapse-{uid}" class="panel-collapse collapse" role="tabpanel" aria-labelledby="heading-{uid}">
      <div class="panel-body">
        {html_header}
      </div>
    </div>
  </div>
        """
        suffix = """
</div>"""
        content = ''.join([element.format(header=header,
                                          uid=header['uid'],
                                          html_header=header._repr_html_(),
                                          repr_uid=repr_uid,
                                          human_time=humanize.naturaldelta(
                                              ttime.time() - header['start_time']))
                           for header in self])
        return prefix + content + suffix
