from __future__ import print_function
import warnings
import six  # noqa
from collections import deque, defaultdict
import uuid
from datetime import datetime
import tzlocal
import pytz
import logging
import numbers
import requests
from doct import Document
from .core import (Header, _external_keys,
                   get_events as _get_events,
                   get_table as _get_table,
                   restream as _restream,
                   fill_event as _fill_event,
                   process as _process, Images,
                   get_fields  # for conveniece
                  )
from metadatastore.core import _format_time


# Toolz and CyToolz have identical APIs -- same test suite, docstrings.
try:
    from cytoolz.dicttoolz import merge
except ImportError:
    from toolz.dicttoolz import merge


try:
    from functools import singledispatch
except ImportError:
    try:
        # We are running on Python 2.6, 2.7, or 3.3
        from singledispatch import singledispatch
    except ImportError:
        raise ImportError(
            "Please install singledispatch from PyPI"
            "\n\n   pip install singledispatch"
            "\n\nThen run your program again."
        )
try:
    from collections.abc import MutableSequence
except ImportError:
    # This will error on python < 3.3
    from collections import MutableSequence


logger = logging.getLogger(__name__)


@singledispatch
def search(key, mds):
    logger.info('Using default search for key = %s' % key)
    raise ValueError("Must give an integer scan ID like [6], a slice "
                     "into past scans like [-5], [-5:], or [-5:-9:2], "
                     "a list like [1, 7, 13], a (partial) uid "
                     "like ['a23jslk'] or a full uid like "
                     "['f26efc1d-8263-46c8-a560-7bf73d2786e1'].")


@search.register(slice)
def _(key, mds):
    # Interpret key as a slice into previous scans.
    logger.info('Interpreting key = %s as a slice' % key)
    if key.start is not None and key.start > -1:
        raise ValueError("slice.start must be negative. You gave me "
                         "key=%s The offending part is key.start=%s"
                         % (key, key.start))
    if key.stop is not None and key.stop > 0:
        raise ValueError("slice.stop must be <= 0. You gave me key=%s. "
                         "The offending part is key.stop = %s"
                         % (key, key.stop))
    if key.stop is not None:
        stop = -key.stop
    else:
        stop = None
    if key.start is None:
        raise ValueError("slice.start cannot be None because we do not "
                         "support slicing infinitely into the past; "
                         "the size of the result is non-deterministic "
                         "and could become too large.")
    start = -key.start
    result = list(mds.find_last(start))[stop::key.step]
    header = [Header.from_run_start(mds, h) for h in result]
    return header


@search.register(numbers.Integral)
def _(key, mds):
    logger.info('Interpreting key = %s as an integer' % key)
    if key > -1:
        # Interpret key as a scan_id.
        gen = mds.find_run_starts(scan_id=key)
        try:
            result = next(gen)  # most recent match
        except StopIteration:
            raise ValueError("No such run found for key=%s which is "
                             "being interpreted as a scan id." % key)
        header = Header.from_run_start(mds, result)
    else:
        # Interpret key as the Nth last scan.
        gen = mds.find_last(-key)
        for i in range(-key):
            try:
                result = next(gen)
            except StopIteration:
                raise IndexError(
                    "There are only {0} runs.".format(i))
        header = Header.from_run_start(mds, result)
    return header


@search.register(str)
@search.register(six.text_type)
@search.register(six.string_types,)
def _(key, mds):
    logger.info('Interpreting key = %s as a str' % key)
    results = None
    if len(key) == 36:
        # Interpret key as a complete uid.
        # (Try this first, for performance.)
        logger.debug('Treating %s as a full uuid' % key)
        results = list(mds.find_run_starts(uid=key))
        logger.debug('%s runs found for key=%s treated as a full uuid'
                     % (len(results), key))
    if not results:
        # No dice? Try searching as if we have a partial uid.
        logger.debug('Treating %s as a partial uuid' % key)
        gen = mds.find_run_starts(uid={'$regex': '{0}.*'.format(key)})
        results = list(gen)
    if not results:
        # Still no dice? Bail out.
        raise ValueError("No such run found for key=%r" % key)
    if len(results) > 1:
        raise ValueError("key=%r matches %d runs. Provide "
                         "more characters." % (key, len(results)))
    result, = results
    header = Header.from_run_start(mds, result)
    return header


@search.register(set)
@search.register(tuple)
@search.register(MutableSequence)
def _(key, mds):
    logger.info('Interpreting key = {} as a set, tuple or MutableSequence'
                ''.format(key))
    return [search(k, mds) for k in key]


class Broker(object):
    def __init__(self, mds, fs, plugins=None, filters=None):
        """
        Unified interface to data sources

        Parameters
        ----------
        mds : metadatastore or metadataclient
        fs : filestore
        plugins : dict or None, optional
            mapping keyword argument name (string) to Plugin, an object
            that should implement ``get_events``
        filters : list
            list of mongo queries to be combined with query using '$and',
            acting as a filter to restrict the results
        """
        self.mds = mds
        self.fs = fs
        if plugins is None:
            plugins = {}
        self.plugins = plugins
        if filters is None:
            filters = []
        self.filters = filters
        self.aliases = {}

    def _format_time(self, val):
        "close over the timezone config"
        # modifies a query dict in place, remove keys 'start_time' and
        # 'stop_time' and adding $lte and/or $gte queries on 'time' key
        _format_time(val, self.mds.config['timezone'])

    @property
    def filters(self):
        return self._filters

    @filters.setter
    def filters(self, val):
        for elem in val:
            self._format_time(elem)
        self._filters = val

    def add_filter(self, **kwargs):
        """
        Add query to the list of 'filter' queries.

        Filter queries are combined with every given query using '$and',
        acting as a filter to restrict the results.

        ``Broker.add_filter(**kwargs)`` is just a convenient way to spell
        ``Broker.filters.append(dict(**kwargs))``.

        Example
        -------
        Filter all searches to restrict runs to a specific 'user'.
        >>> db.add_filter(user='Dan')

        See Also
        --------
        `Broker.add_filter`

        """
        self.filters.append(dict(**kwargs))

    def clear_filters(self, **kwargs):
        """
        Clear all 'filter' queries.

        Filter queries are combined with every given query using '$and',
        acting as a filter to restrict the results.

        ``Broker.clear_filters()`` is just a convenient way to spell
        ``Broker.filters.clear()``.

        See Also
        --------
        `Broker.add_filter`
        """
        self.filters.clear()

    def __getitem__(self, key):
        """Do-What-I-Mean slicing"""
        return search(key, self.mds)

    def __getattr__(self, key):
        try:
            query = self.aliases[key]
        except KeyError:
            raise AttributeError(key)
        if callable(query):
            query = query()
        return self(**query)

    def alias(self, key, **query):
        if hasattr(self, key) and key not in self.aliases:
            raise ValueError("'%s' is not a legal alias." % key)
        self.aliases[key] = query

    def dynamic_alias(self, key, func):
        if hasattr(self, key) and key not in self.aliases:
            raise ValueError("'%s' is not a legal alias." % key)
        self.aliases[key] = func

    def __call__(self, text_search=None, **kwargs):
        """Given search criteria, find Headers describing runs.

        This function returns a list of dictionary-like objects encapsulating
        the metadata for a run -- start time, instruments used, and so on.
        In addition to the Parameters below, advanced users can specifiy
        arbitrary queries that are passed through to mongodb.

        Parameters
        ----------
        text_search : str, optional
            search full text of RunStart documents
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
        data_key : str, optional
            The alias (e.g., 'motor1') or PV identifier of data source

        Returns
        -------
        data : list
            Header objects

        Examples
        --------
        >>> DataBroker('keyword')  # full text search
        >>> DataBroker(start_time='2015-03-05', stop_time='2015-03-10')
        >>> DataBroker(data_key='motor1')
        >>> DataBroker(data_key='motor1', start_time='2015-03-05')
        """
        data_key = kwargs.pop('data_key', None)
        if text_search is not None:
            query = {'$and': [{'$text': {'$search': text_search}}]
                               + self.filters}
        else:
            # Include empty {} here so that '$and' gets at least one query.
            self._format_time(kwargs)
            query = {'$and': [{}] + [kwargs] + self.filters}
        run_start = self.mds.find_run_starts(**query)

        headers = []
        for rs in run_start:
            header = Header.from_run_start(self.mds, rs)
            if data_key is None:
                headers.append(header)
                continue
            else:
                # Only include this header in the result if `data_key` is found
                # in one of its descriptors' data_keys.
                for descriptor in header.descriptors:
                    if data_key in descriptor['data_keys']:
                        headers.append(header)
                        break
        return headers

    def find_headers(self, **kwargs):
        "This function is deprecated."
        warnings.warn("Use .__call__() instead of .find_headers()")
        return self(**kwargs)

    def fetch_events(self, headers, fill=True):
        "This function is deprecated."
        warnings.warn("Use .get_events() instead.")
        return self.get_events(headers, fill=fill)

    def fill_event(self, event, handler_registry=None, handler_overrides=None):
        """
        Populate events with externally stored data.

        Parameters
        ----------
        event : document
        handler_registry : dict, optional
            mapping spec names (strings) to handlers (callable classes)
        handler_overrides : dict, optional
            mapping data keys (strings) to handlers (callable classes)
        """
        _fill_event(self.fs, event, handler_registry=handler_registry,
                    handler_overrides=handler_overrides)

    def get_events(self, headers, fields=None, stream_name=None, fill=False,
                   handler_registry=None, handler_overrides=None, **kwargs):
        """
        Get Events from given run(s).

        Parameters
        ----------
        headers : Header or iterable of Headers
            The headers to fetch the events for
        fields : list, optional
            whitelist of field names of interest; if None, all are returned
        fill : bool, optional
            Whether externally-stored data should be filled in. Defaults to True
        stream_name : string, optional
            Get events from only one "event stream" with this name. If None
            (default) get events from all event streams.
        handler_registry : dict, optional
            mapping filestore specs (strings) to handlers (callable classes)
        handler_overrides : dict, optional
            mapping data keys (strings) to handlers (callable classes)
        kwargs
            passed through the any plugins

        Yields
        ------
        event : Event
            The event, optionally with non-scalar data filled in

        Raises
        ------
        ValueError if any key in `fields` is not in at least one descriptor pre header.
        """
        res = _get_events(mds=self.mds, fs=self.fs, headers=headers,
                         fields=fields, stream_name=stream_name, fill=fill,
                         handler_registry=handler_registry,
                         handler_overrides=handler_overrides,
                         plugins=self.plugins, **kwargs)
        for event in res:
            yield event

    def get_table(self, headers, fields=None, stream_name='primary',
                  fill=False,
                  convert_times=True, timezone=None, handler_registry=None,
                  handler_overrides=None):
        """
        Make a table (pandas.DataFrame) from given run(s).

        Parameters
        ----------
        headers : Header or iterable of Headers
            The headers to fetch the events for
        fields : list, optional
            whitelist of field names of interest; if None, all are returned
        stream_name : string, optional
            Get data from a single "event stream." To obtain one comprehensive
            table with all streams, use `stream_name=None`. The default name is
            'primary', but if no event stream with that name is found, the
            default reverts to `None` (for backward-compatibility).
        fill : bool, optional
            Whether externally-stored data should be filled in.
            Defaults to True
        convert_times : bool, optional
            Whether to convert times from float (seconds since 1970) to
            numpy datetime64, using pandas. True by default.
        timezone : str, optional
            e.g., 'US/Eastern'; if None, use metadatastore configuration in
            `self.mds.config['timezone']`
        handler_registry : dict, optional
            mapping filestore specs (strings) to handlers (callable classes)
        handler_overrides : dict, optional
            mapping data keys (strings) to handlers (callable classes)

        Returns
        -------
        table : pandas.DataFrame
        """
        if timezone is None:
            timezone = self.mds.config['timezone']
        res = _get_table(mds=self.mds, fs=self.fs, headers=headers,
                         fields=fields, stream_name=stream_name, fill=fill,
                         convert_times=convert_times,
                         timezone=timezone, handler_registry=handler_registry,
                         handler_overrides=handler_overrides)
        return res

    def get_images(self, headers, name, handler_registry=None,
                   handler_override=None):
        """
        Load images from a detector for given Header(s).

        Parameters
        ----------
        fs: FileStoreRO
        headers : Header or list of Headers
        name : string
            field name (data key) of a detector
        handler_registry : dict, optional
            mapping spec names (strings) to handlers (callable classes)
        handler_override : callable class, optional
            overrides registered handlers


        Example
        -------
        >>> header = DataBroker[-1]
        >>> images = Images(header, 'my_detector_lightfield')
        >>> for image in images:
                # do something
        """
        return Images(self.mds, self.fs, headers, name, handler_registry,
                      handler_override)

    def get_resource_uids(self, header):
        '''Given a Header, give back a list of resource uids

        These uids are required to move the underlying files.

        Parameters
        ----------
        header : Header

        Returns
        -------
        ret : set
            set of resource uids which are refereneced by this
            header.
        '''
        external_keys = set()
        for d in header['descriptors']:
            for k, v in d['data_keys'].items():
                if 'external' in v:
                    external_keys.add(k)

        ev_gen = self.get_events(header, stream_name=None,
                                 fields=external_keys, fill=False)
        resources = set()
        for ev in ev_gen:
            for v in ev['data'].values():
                res = self.fs.resource_given_eid(v)
                resources.add(res['uid'])
        return resources

    def restream(self, headers, fields=None, fill=False):
        """
        Get all Documents from given run(s).

        Parameters
        ----------
        headers : Header or iterable of Headers
            header or headers to fetch the documents for
        fields : list, optional
            whitelist of field names of interest; if None, all are returned
        fill : bool, optional
            Whether externally-stored data should be filled in. Defaults to
            False.

        Yields
        ------
        name, doc : tuple
            string name of the Document type and the Document itself.
            Example: ('start', {'time': ..., ...})

        Example
        -------
        >>> def f(name, doc):
        ...     # do something
        ...
        >>> h = DataBroker[-1]  # most recent header
        >>> for name, doc in restream(h):
        ...     f(name, doc)

        Note
        ----
        This output can be used as a drop-in replacement for the output of the
        bluesky Run Engine.

        See Also
        --------
        process
        """
        res = _restream(self.mds, self.fs, headers, fields=fields, fill=fill)
        for name_doc_pair in res:
            yield name_doc_pair

    stream = restream  # compat

    def process(self, headers, func, fields=None, fill=False):
        """
        Get all Documents from given run to a callback.

        Parameters
        ----------
        headers : Header or iterable of Headers
            header or headers to process documents from
        func : callable
            function with the signature `f(name, doc)`
            where `name` is a string and `doc` is a dict
        fields : list, optional
            whitelist of field names of interest; if None, all are returned
        fill : bool, optional
            Whether externally-stored data should be filled in. Defaults to
            False.

        Example
        -------
        >>> def f(name, doc):
        ...     # do something
        ...
        >>> h = DataBroker[-1]  # most recent header
        >>> process(h, f)

        Note
        ----
        This output can be used as a drop-in replacement for the output of the
        bluesky Run Engine.

        See Also
        --------
        restream
        """
        _process(mds=self.mds, fs=self.fs, headers=headers, func=func,
                 fields=fields, fill=fill)

    get_fields = staticmethod(get_fields)  # for convenience


class ArchiverPlugin(object):
    def __init__(self, url, timezone):
        """
        DataBroker plugin

        Parameters
        ----------
        url : string
            e.g., 'http://host:port/'
        timezone : string
            e.g., 'US/Eastern'

        Example
        -------
        >>> p = ArchiverPlugin('http://xf16idc-ca.cs.nsls2.local:17668/',
        ...                    'US/Eastern')
        >>> db = Broker(mds, fs, plugins={'archiver_pvs': p})
        >>> header = db[-1]
        >>> db.get_events(header, archiver_pvs=['...'])
        """
        if not url.endswith('/'):
            url += '/'
        self.url = url
        self.archiver_addr = self.url + "retrieval/data/getData.json"
        self.tz = pytz.timezone(timezone)

    def get_events(self, header, pvs):
        """
        Return results of an EPICS Archiver Appliance query in Event documents.

        That is, mock Event documents so that data from Archiver can be
        analyzed the same as data from metadatastore.

        Parameters
        ----------
        header : Header
        pvs : list or dict
            a list of PVs or a dict mapping PVs to human-friendly names
        """
        if hasattr(pvs, 'items'):
            # Interpret pvs as a dict mapping PVs to names.
            pass
        else:
            # Interpret pvs as a list, and use PVs themselves as names.
            pvs = {pv: pv for pv in pvs}
        start_time, stop_time = header['start']['time'], header['stop']['time']
        for pv, name in pvs.items():
            _from = _munge_time(start_time, self.tz)
            _to = _munge_time(stop_time, self.tz)
            params = {'pv': pv, 'from': _from, 'to': _to}
            req = requests.get(self.archiver_addr, params=params, stream=True)
            req.raise_for_status()
            raw, = req.json()
            timestamps = [x['secs'] for x in raw['data']]
            data = [x['val'] for x in raw['data']]
            # Roll these into an Event document.
            descriptor = {'time': start_time,
                          'uid': 'ephemeral-' + str(uuid.uuid4()),
                          'data_keys': {name: {'source': pv, 'shape': [],
                                               'dtype': 'number'}},
                          # TODO Mark this as 'external' once Broker stops
                          # assuming that all external data in is filestore.
                          'run_start': header['start'],
                          'external_query': params,
                          'external_url': self.url}
            descriptor = Document('EventDescriptor', descriptor)
            for d, t in zip(data, timestamps):
                doc = {'data': {name: d}, 'timestamps': {name: t}, 'time': t,
                       'uid': 'ephemeral-' + str(uuid.uuid4()),
                       'descriptor': descriptor}
                yield Document('Event', doc)


def _munge_time(t, timezone):
    """Close your eyes and trust @arkilic

    Parameters
    ----------
    t : float
        POSIX (seconds since 1970)
    timezone : pytz object
        e.g. ``pytz.timezone('US/Eastern')``

    Return
    ------
    time
        as ISO-8601 format
    """
    t = datetime.fromtimestamp(t)
    return timezone.localize(t).replace(microsecond=0).isoformat()
