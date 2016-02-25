from __future__ import print_function
import warnings
import six  # noqa
from collections import deque, defaultdict
from itertools import chain
import pandas as pd
import tzlocal
from metadatastore.commands import (find_last, find_run_starts,
                                    find_descriptors,
                                    get_events_generator, get_events_table)
import doct as doc
import metadatastore.commands as mc
import filestore.api as fs
import logging
import numbers

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
TZ = str(tzlocal.get_localzone())


@singledispatch
def search(key):
    logger.info('Using default search for key = %s' % key)
    raise ValueError("Must give an integer scan ID like [6], a slice "
                     "into past scans like [-5], [-5:], or [-5:-9:2], "
                     "a list like [1, 7, 13], a (partial) uid "
                     "like ['a23jslk'] or a full uid like "
                     "['f26efc1d-8263-46c8-a560-7bf73d2786e1'].")


@search.register(slice)
def _(key):
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
    result = list(find_last(start))[stop::key.step]
    header = [Header.from_run_start(h) for h in result]
    return header


@search.register(numbers.Integral)
def _(key):
    logger.info('Interpreting key = %s as an integer' % key)
    if key > -1:
        # Interpret key as a scan_id.
        gen = find_run_starts(scan_id=key)
        try:
            result = next(gen)  # most recent match
        except StopIteration:
            raise ValueError("No such run found for key=%s which is "
                             "being interpreted as a scan id." % key)
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
    return header


@search.register(str)
@search.register(six.text_type)
# py2: six.string_types = (basestring,)
# py3: six.string_types = (str,)
# so we need to just grab the only element out of this
@search.register(six.string_types,)
def _(key):
    logger.info('Interpreting key = %s as a str' % key)
    results = None
    if len(key) >= 36:
        # Interpret key as a uid (or the few several characters of one).
        logger.debug('Treating %s as a full uuid' % key)
        results = list(find_run_starts(uid=key))
        logger.debug('%s runs found for key=%s treated as a full uuid'
                     % (len(results), key))
    if not results == 0:
        # No dice? Try searching as if we have a partial uid.
        logger.debug('Treating %s as a partial uuid' % key)
        gen = find_run_starts(uid={'$regex': '{0}.*'.format(key)})
        results = list(gen)
    if not results:
        # Still no dice? Bail out.
        raise ValueError("No such run found for key=%s" % key)
    if len(results) > 1:
        raise ValueError("key=%s  matches %s runs. Provide "
                         "more characters." % (key, len(results)))
    result, = results
    header = Header.from_run_start(result)
    return header


@search.register(set)
@search.register(tuple)
@search.register(MutableSequence)
def _(key):
    logger.info('Interpreting key = {} as a set, tuple or MutableSequence'
                ''.format(key))
    return [search(k) for k in key]


class _DataBrokerClass(object):
    # A singleton is instantiated in broker/__init__.py.
    # You probably do not want to instantiate this; use
    # broker.DataBroker instead.
    def __getitem__(self, key):
        """DWIM slicing

        Some more docs go here
        """
        return search(key)

    def __call__(self, **kwargs):
        """Given search criteria, find Headers describing runs.

        This function returns a list of dictionary-like objects encapsulating
        the metadata for a run -- start time, instruments used, and so on.
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
        data_key : str, optional
            The alias (e.g., 'motor1') or PV identifier of data source

        Returns
        -------
        data : list
            Header objects

        Examples
        --------
        >>> DataBroker(start_time='2015-03-05', stop_time='2015-03-10')
        >>> DataBroker(data_key='motor1')
        >>> DataBroker(data_key='motor1', start_time='2015-03-05')
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
                if descriptor['run_start']['uid'] not in known_uids:
                    rs = descriptor['run_start']
                    known_uids.append(rs['uid'])
                    result.append(rs)
            run_start = result
        result = []
        for rs in run_start:
            result.append(Header.from_run_start(rs))
        return result

    def find_headers(self, **kwargs):
        "This function is deprecated. Use DataBroker() instead."
        warnings.warn("Use DataBroker() instead of "
                      "DataBroker.find_headers()", UserWarning)
        return self(**kwargs)

    def fetch_events(self, headers, fill=True):
        "This function is deprecated. Use top-level function get_events."
        warnings.warn("Use top-level function "
                                   "get_events() instead.", UserWarning)
        return get_events(headers, None, fill)


DataBroker = _DataBrokerClass()  # singleton, used by pims_readers import below


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


def fill_event(event, handler_registry=None, handler_overrides=None):
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
    if handler_overrides is None:
        handler_overrides = {}
    is_external = _inspect_descriptor(event.descriptor)
    mock_registries = {data_key: defaultdict(lambda: handler)
                       for data_key, handler in handler_overrides.items()}
    for data_key, value in six.iteritems(event.data):
        if is_external.get(data_key, False):
            if data_key not in handler_overrides:
                event.data[data_key] = fs.retrieve(value, handler_registry)
            else:
                mock_registry = mock_registries[data_key]
                event.data[data_key] = fs.retrieve(value, mock_registry)


class Header(doc.Document):
    """A dictionary-like object summarizing metadata for a run."""

    @classmethod
    def from_run_start(cls, run_start, verify_integrity=False):
        """
        Build a Header from a RunStart Document.

        Parameters
        ----------
        run_start : metadatastore.document.Document or str
            RunStart Document or uid

        Returns
        -------
        header : databroker.broker.Header
        """
        run_start_uid = mc.doc_or_uid_to_uid(run_start)
        run_start = mc.run_start_given_uid(run_start_uid)

        try:
            run_stop = doc.ref_doc_to_uid(mc.stop_by_start(run_start_uid),
                                          'run_start')
        except mc.NoRunStop:
            run_stop = None

        try:
            ev_descs = [doc.ref_doc_to_uid(ev_desc, 'run_start')
                        for ev_desc in
                        mc.descriptors_by_start(run_start_uid)]
        except mc.NoEventDescriptors:
            ev_descs = []

        d = {'start': run_start, 'stop': run_stop, 'descriptors': ev_descs}
        return cls('header', d)


def get_events(headers, fields=None, fill=True, handler_registry=None,
               handler_overrides=None):
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
    handler_registry : dict, optional
        mapping filestore specs (strings) to handlers (callable classes)
    handler_overrides : dict, optional
        mapping data keys (strings) to handlers (callable classes)

    Yields
    ------
    event : Event
        The event, optionally with non-scalar data filled in

    Raises
    ------
    ValueError if any key in `fields` is not in at least one descriptor pre header.
    """
    # A word about the 'fields' argument:
    # Notice that we assume that the same field name cannot occur in
    # more than one descriptor. We could relax this assumption, but
    # we current enforce it in bluesky, so it is safe for now.
    try:
        headers.items()
    except AttributeError:
        pass
    else:
        headers = [headers]

    if fields is None:
        fields = []
    fields = set(fields)
    _check_fields_exist(fields, headers)

    for header in headers:
        # cache these attribute look-ups for performance
        start = header['start']
        stop = header['stop']
        if stop is None:
            stop = {}
        for descriptor in header['descriptors']:
            objs_config = descriptor.get('configuration', {}).values()
            config_data = merge(obj_conf['data'] for obj_conf in objs_config)
            config_ts = merge(obj_conf['timestamps']
                              for obj_conf in objs_config)
            discard_fields = set()
            extra_fields = set()
            if fields:
                event_fields = set(descriptor['data_keys'])
                discard_fields = event_fields - fields
                extra_fields = fields - event_fields
            for event in get_events_generator(descriptor):
                event_data = event.data  # cache for perf
                event_timestamps = event.timestamps
                for field in discard_fields:
                    del event_data[field]
                    del event_timestamps[field]
                for field in extra_fields:
                    # Look in the descriptor, then start, then stop.
                    if field in config_data:
                        event_data[field] = config_data[field]
                        event_timestamps[field] = config_ts[field]
                    elif field in start:
                        event_data[field] = start[field]
                        event_timestamps[field] = start['time']
                    elif field in stop:
                        event_data[field] = stop[field]
                        event_timestamps[field] = stop['time']
                    # (else omit it from the events of this descriptor)
                if fill:
                    fill_event(event, handler_registry, handler_overrides)
                yield event


def get_table(headers, fields=None, fill=True, convert_times=True,
              handler_registry=None, handler_overrides=None):
    """
    Make a table (pandas.DataFrame) from given run(s).

    Parameters
    ----------
    headers : Header or iterable of Headers
        The headers to fetch the events for
    fields : list, optional
        whitelist of field names of interest; if None, all are returned
    fill : bool, optional
        Whether externally-stored data should be filled in. Defaults to True
    convert_times : bool, optional
        Whether to convert times from float (seconds since 1970) to
        numpy datetime64, using pandas. True by default.
    handler_registry : dict, optional
        mapping filestore specs (strings) to handlers (callable classes)
    handler_overrides : dict, optional
        mapping data keys (strings) to handlers (callable classes)

    Returns
    -------
    table : pandas.DataFrame
    """
    # A word about the 'fields' argument:
    # Notice that we assume that the same field name cannot occur in
    # more than one descriptor. We could relax this assumption, but
    # we current enforce it in bluesky, so it is safe for now.
    try:
        headers.items()
    except AttributeError:
        pass
    else:
        headers = [headers]

    if handler_overrides is None:
        handler_overrides = {}

    if fields is None:
        fields = []
    fields = set(fields)
    _check_fields_exist(fields, headers)

    dfs = []
    for header in headers:
        # cache these attribute look-ups for performance
        start = header['start']
        stop = header['stop']
        if stop is None:
            stop = {}
        for descriptor in header['descriptors']:
            is_external = _inspect_descriptor(descriptor)
            objs_config = descriptor.get('configuration', {}).values()
            config_data = merge(obj_conf['data'] for obj_conf in objs_config)
            discard_fields = set()
            extra_fields = set()
            if fields:
                event_fields = set(descriptor['data_keys'])
                discard_fields = event_fields - fields
                extra_fields = fields - event_fields
            payload = get_events_table(descriptor)
            descriptor, data, seq_nums, times, uids, timestamps = payload
            df = pd.DataFrame(index=seq_nums)
            if convert_times:
                times = pd.to_datetime(
                    pd.Series(times, index=seq_nums),
                    unit='s', utc=True).dt.tz_localize(TZ)
            df['time'] = times
            for field in discard_fields:
                logger.debug('Discarding field %s', field)
                del df[field]
            for field in df.columns:
                if is_external.get(field) and fill:
                    logger.debug('filling data for %s', field)
                    # TODO someday we will have bulk retrieve in FS
                    datum_uids = df[field]
                    if field not in handler_overrides:
                        values = [fs.retrieve(value) for value in datum_uids]
                    else:
                        handler = handler_overrides[field]
                        mock_registry = defaultdict(lambda: handler)
                        values = [fs.retrieve(value, mock_registry)
                                  for value in datum_uids]
                    df[field] = values
            for field in extra_fields:
                # Look in the descriptor, then start, then stop.
                # Broadcast any values through the whole df.
                if field in config_data:
                    df[field] = config_data[field]
                elif field in start:
                    df[field] = start[field]
                elif field in stop:
                    df[field] = stop[field]
                # (else omit it from the events of this descriptor)
            dfs.append(df)
    if dfs:
        return pd.concat(dfs)
    else:
        # edge case: no data
        return pd.DataFrame()


def restream(headers, fields=None, fill=True):
    """
    Get all Documents from given run(s).

    Parameters
    ----------
    headers : Header or iterable of Headers
        header or headers to fetch the documents for
    fields : list, optional
        whitelist of field names of interest; if None, all are returned
    fill : bool, optional
        Whether externally-stored data should be filled in. Defaults to True

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
    try:
        headers.items()
    except AttributeError:
        pass
    else:
        headers = [headers]

    for header in headers:
        yield 'start', header['start']
        for descriptor in header['descriptors']:
            yield 'descriptor', descriptor
        # When py2 compatibility is dropped, use yield from.
        for event in get_events(header, fields=fields, fill=fill):
            yield 'event', event
        yield 'stop', header['stop']


stream = restream  # compat


def process(headers, func, fields=None, fill=True):
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
        Whether externally-stored data should be filled in. Defaults to True

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
    for name, doc in restream(headers, fields, fill):
        func(name, doc)


def get_fields(header):
    """
    Return the set of all field names (a.k.a "data keys") in a header.

    Parameters
    ----------
    header : Header

    Returns
    -------
    fields : set
    """
    fields = set()
    for desc in header['descriptors']:
        for field in desc['data_keys'].keys():
            fields.add(field)
    return fields

def _check_fields_exist(fields, headers):
    all_fields = set()
    for header in headers:
        all_fields.update(header['start'])
        stop = header['stop']
        if stop is not None:
            all_fields.update(header['stop'])
        for descriptor in header['descriptors']:
            all_fields.update(descriptor['data_keys'])
            objs_conf = descriptor.get('configuration', {})
            config_fields = [obj_conf['data'] for obj_conf in objs_conf.values()]
            all_fields.update(chain(*config_fields))
    missing = fields - all_fields
    if missing:
        raise ValueError("The fields %r were not found." % missing)
