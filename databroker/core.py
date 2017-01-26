from __future__ import print_function
import six  # noqa
from collections import defaultdict, deque
from itertools import chain
import pandas as pd
import doct
from pims import FramesSequence, Frame
import logging
import boltons.cacheutils
import re
import attr

# Toolz and CyToolz have identical APIs -- same test suite, docstrings.
try:
    from cytoolz.dicttoolz import merge
except ImportError:
    from toolz.dicttoolz import merge


logger = logging.getLogger(__name__)


class ALL:
    "Sentinel used as the default value for stream_name"
    pass


class InvalidDocumentSequence(Exception):
    pass


def fill_event(fs, event, handler_registry=None, handler_overrides=None):
    """
    Populate events with externally stored data.

    Parameters
    ----------
    fs : FileStoreRO
    event : document
    handler_registry : dict, optional
        mapping spec names (strings) to handlers (callable classes)
    handler_overrides : dict, optional
        mapping data keys (strings) to handlers (callable classes)

    .. warning

       This mutates the event's ``data`` field in-place
    """
    if handler_overrides is None:
        handler_overrides = {}
    if handler_registry is None:
        handler_registry = {}
    is_external = _external_keys(event.descriptor)
    mock_registries = {data_key: defaultdict(lambda: handler)
                       for data_key, handler in handler_overrides.items()}
    for data_key, value in six.iteritems(event.data):
        if is_external.get(data_key) is not None:
            if data_key not in handler_overrides:
                with fs.handler_context(handler_registry) as _fs:
                    event.data[data_key] = _fs.get_datum(value)
                    event.filled[data_key] = True
            else:
                mock_registry = mock_registries[data_key]
                with fs.handler_context(mock_registry) as _fs:
                    event.data[data_key] = _fs.get_datum(value)
                    event.filled[data_key] = True


@attr.s(frozen=True)
class Header(object):
    """A dictionary-like object summarizing metadata for a run."""

    _name = 'header'
    db = attr.ib(cmp=False, hash=False)
    start = attr.ib()
    stop = attr.ib(default=attr.Factory(dict))
    _cache = attr.ib(default=attr.Factory(dict), cmp=False, hash=False)

    @classmethod
    def from_run_start(cls, db, run_start):
        """
        Build a Header from a RunStart Document.

        Parameters
        ----------
        db : DataBroker

        run_start : dict or string
            RunStart document or uid of one

        Returns
        -------
        header : databroker.broker.Header
        """
        mds = db.hs.mds
        if isinstance(run_start, six.string_types):
            run_start = mds.run_start_given_uid(run_start)
        run_start_uid = run_start['uid']

        try:
            run_stop = doct.ref_doc_to_uid(mds.stop_by_start(run_start_uid),
                                           'run_start')
        except mds.NoRunStop:
            run_stop = None

        d = {'start': run_start}
        if run_stop is not None:
            d['stop'] = run_stop
        h = cls(db, **d)
        return h

    @property
    def descriptors(self):
        if 'desc' not in self._cache:
            self._cache['desc'] = sum((es.descriptors_given_header(self)
                                       for es in self.db.event_sources), [])
        return self._cache['desc']

    def __getitem__(self, k):
        try:
            return getattr(self, k)
        except AttributeError as e:
            raise KeyError(k)

    def get(self, *args, **kwargs):
        return getattr(self, *args, **kwargs)

    def items(self):
        for k in self.keys():
            yield k, getattr(self, k)

    def values(self):
        for k in self.keys():
            yield getattr(self, k)

    def keys(self):
        for k in ('start', 'descriptors', 'stop'):
            yield k

    def to_name_dict_pair(self):
        ret = attr.asdict(self)
        ret.pop('db')
        ret.pop('_cache')
        ret['descriptors'] = self.descriptors
        return self._name, ret

    def __str__(self):
        return doct.vstr(self)

    def __len__(self):
        return 3

    def __iter__(self):
        return self.keys()

    def stream(self, stream_name=ALL, fill=True):
        # TODO hit the plugins
        gen = self.db.es.docs_given_header(
            header=self,
            stream_name=stream_name,
            fill=fill)
        for payload in gen:
            yield payload


def restream(mds, fs, es, headers, fields=None, fill=False):
    """
    Get all Documents from given run(s).

    Parameters
    ----------
    mds : MDSRO
    fs : FileStoreRO
    es : EventStoreRO
    headers : Header or iterable of Headers
        header or headers to fetch the documents for
    fields : list, optional
        whitelist of field names of interest; if None, all are returned
    fill : bool, optional
        Whether externally-stored data should be filled in. Defaults to False.

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
        for event in header.db.get_events(header, fields=fields, fill=fill):
            yield 'event', event
        yield 'stop', header['stop']


stream = restream  # compat


def process(mds, fs, es, headers, func, fields=None, fill=False):
    """
    Get all Documents from given run to a callback.

    Parameters
    ----------
    mds : MDSRO
    fs : FileStoreRO
    es : EventStoreRO
    headers : Header or iterable of Headers
        header or headers to process documents from
    func : callable
        function with the signature `f(name, doc)`
        where `name` is a string and `doc` is a dict
    fields : list, optional
        whitelist of field names of interest; if None, all are returned
    fill : bool, optional
        Whether externally-stored data should be filled in. Defaults to False.

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
    for name, doc in restream(mds, fs, es, headers, fields, fill):
        func(name, doc)


def register_builtin_handlers(fs):
    "Register all the handlers built in to filestore."
    from filestore import handlers, HandlerBase
    # TODO This will blow up if any non-leaves in the class heirarchy
    # have non-empty specs. Make this smart later.
    for cls in vars(handlers).values():
        if isinstance(cls, type) and issubclass(cls, HandlerBase):
            logger.debug("Found Handler %r for specs %r", cls, cls.specs)
            for spec in cls.specs:
                logger.debug("Registering Handler %r for spec %r", cls, spec)
                fs.register_handler(spec, cls)


def get_fields(header, name=None):
    """
    Return the set of all field names (a.k.a "data keys") in a header.

    Parameters
    ----------
    header : Header
    name : string, optional
        Get field from only one "event stream" with this name. If None
        (default) get fields from all event streams.

    Returns
    -------
    fields : set
    """
    fields = set()
    for descriptor in header['descriptors']:
        if name is not None and name != descriptor.get('name'):
            continue
        for field in descriptor['data_keys'].keys():
            fields.add(field)
    return fields


def _compile_re(fields=[]):
    """
    Return a regular expression object based on a list of regular expressions.

    Parameters
    ----------
    fields : list, optional
        List of regular expressions. If fields is empty returns a general RE.

    Returns
    -------
    comp_re : regular expression object

    """
    if len(fields) == 0:
        fields = ['.*']
    f = ["(?:" + regex + r")\Z" for regex in fields]
    comp_re = re.compile('|'.join(f))
    return comp_re


def _check_fields_exist(fields, headers):
    comp_re = _compile_re(fields)
    all_fields = set()
    for header in headers:
        all_fields.update(header['start'])
        all_fields.update(header.get('stop', {}))
        for descriptor in header['descriptors']:
            all_fields.update(descriptor['data_keys'])
            objs_conf = descriptor.get('configuration', {})
            config_fields = [obj_conf['data'] for obj_conf in objs_conf.values()]
            all_fields.update(chain(*config_fields))
    missing = len(set(filter(comp_re.match,all_fields))) > 0
    if not missing:
        raise ValueError("The fields %r were not found." %fields)


def get_images(fs, headers, name, handler_registry=None,
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
    return Images(mds, fs, headers, name, handler_registry, handler_override)


class Images(FramesSequence):
    def __init__(self, mds, fs, es, headers, name, handler_registry=None,
                 handler_override=None):
        """
        Load images from a detector for given Header(s).

        Parameters
        ----------
        fs : FileStoreRO
        headers : Header or list of Headers
        es : EventStoreRO
        name : str
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
        from .broker import Broker
        self.fs = fs
        db = Broker(mds, fs)
        events = db.get_events(headers, [name], fill=False)

        self._datum_uids = [event.data[name] for event in events
                            if name in event.data]
        self._len = len(self._datum_uids)
        first_uid = self._datum_uids[0]
        if handler_override is None:
            self.handler_registry = handler_registry
        else:
            # mock a handler registry
            self.handler_registry = defaultdict(lambda: handler_override)
        with self.fs.handler_context(self.handler_registry) as fs:
            example_frame = fs.get_datum(first_uid)
        # Try to duck-type as a numpy array, but fall back as a general
        # Python object.
        try:
            self._dtype = example_frame.dtype
        except AttributeError:
            self._dtype = type(example_frame)
        try:
            self._shape = example_frame.shape
        except AttributeError:
            self._shape = None  # as in, unknown

    @property
    def pixel_type(self):
        return self._dtype

    @property
    def frame_shape(self):
        return self._shape

    def __len__(self):
        return self._len

    def get_frame(self, i):
        with self.fs.handler_context(self.handler_registry) as fs:
            img = fs.get_datum(self._datum_uids[i])
        if hasattr(img, '__array__'):
            return Frame(img, frame_no=i)
        else:
            # some non-numpy-like type
            return img


def _external_keys(descriptor, _cache=boltons.cacheutils.LRU(max_size=500)):
    """Which data keys are stored externally

    Parameters
    ----------
    descriptor : Doct
        The descriptor

    Returns
    -------
    external_keys : dict
        Maps data key -> the value of external field or None if the
        field does not exist.
    """
    try:
        ek = _cache[descriptor['uid']]
    except KeyError:
        data_keys = descriptor['data_keys']
        ek = {k: v.get('external', None) for k, v in data_keys.items()}
        _cache[descriptor['uid']] = ek
    return ek


class DocBuffer:
    '''Buffer a (name, document) sequence into parts

    '''
    def __init__(self, doc_gen, denormalize=False):

        class InnerDict(dict):
            def __getitem__(inner_self, key):
                while key not in inner_self:
                    try:
                        self._get_next()
                    except StopIteration:
                        raise Exception("this stream does not contain a "
                                        "descriptor with uid {}".format(key))
                return super().__getitem__(key)

        self.denormalize = denormalize
        self.gen = doc_gen
        self._start = None
        self._stop = None
        self.descriptors = InnerDict()
        self._events = deque()

    @property
    def start(self):
        while self._start is None:
            try:
                self._get_next()
            except StopIteration:
                raise InvalidDocumentSequence(
                    "stream does not contain a start?!")

        return self._start

    @property
    def stop(self):
        while self._stop is None:
            try:
                self._get_next()
            except StopIteration:
                raise InvalidDocumentSequence(
                    "stream does not contain a start")

        return self._stop

    def _get_next(self):
        self.__stash_values(*next(self.gen))

    def __stash_values(self, name, doc):
        if name == 'start':
            if self._start is not None:
                raise Exception("only one start allowed")
            self._start = doc
        elif name == 'stop':
            if self._stop is not None:
                raise Exception("only one stop allowed")
            self._stop = doc
        elif name == 'descriptor':
            self.descriptors[doc['uid']] = doc
        elif name == 'event':
            self._events.append(doc)
        else:
            raise ValueError("{} is unknown document type".format(name))

    def __denormalize(self, ev):
        ev = dict(ev)
        desc = ev['descriptor']
        try:
            ev['descriptor'] = self.descriptors[desc]
        except StopIteration:
            raise InvalidDocumentSequence(
                "{} is on an event, but not in event stream".format(desc))
        return ev

    def __iter__(self):
        gen = self.gen
        while True:
            while len(self._events):
                ev = self._events.popleft()
                if self.denormalize:
                    ev = self.__denormalize(ev)
                yield ev

            try:
                name, doc = next(gen)
            except StopIteration:
                break

            if name == 'event':
                if self.denormalize:
                    doc = self.__denormalize(doc)
                yield doc
            else:
                self.__stash_values(name, doc)


def _project_header_data(source_data, source_ts, selected_fields, comp_re):
    """Extract values from a header for merging into events

    Parameters
    ----------
    source : dict
    selected_fields : set
    comp_re : SRE_Pattern

    Returns
    -------
    data_keys : dict
    data : dict
    timestamps : dict
    """
    fields = (set(filter(comp_re.match, source_data)) - selected_fields)
    data = {k: source_data[k] for k in fields}
    timestamps = {k: source_ts[k] for k in fields}

    return {}, data, timestamps


class EventSourceShim(object):
    '''Shim class to turn a mds object into a EventSource

    This will presumably be deleted if this API makes it's way back down
    into the implementations
    '''
    def __init__(self, mds, fs):
        self.mds = mds
        self.fs = fs

    def insert(self, name, doc):
        return self.mds.insert(name, doc)

    def streams_given_header(self, header):
        return set(d['name'] for d in
                   self.descriptors_given_header(header))

    def descriptors_given_header(self, header):
        return list(desc for desc in
                    self.mds.descriptors_by_start(header.start['uid']))

    def descriptors_given_stream(self, header, stream_name):
        return [d for d in self.descriptors_given_header(header)
                if stream_name is ALL or d['name'] == stream_name]

    def docs_given_header(self, header, stream_name,
                            fill=False, fields=None,
                            **kwargs):
        no_fields_filter = False
        if fields is None:
            no_fields_filter = True
            fields = []
        fields = set(fields)

        comp_re = _compile_re(fields)

        descs = self.descriptors_given_stream(header, stream_name)

        start = header.start
        stop = header.stop

        yield 'start', header.start
        for d in descs:
            (all_extra_dk, all_extra_data,
             all_extra_ts, discard_fields) = _extract_extra_data(
                 start, stop, d, fields, comp_re, no_fields_filter)

            d = d.copy()
            dict.__setitem__(d, 'data_keys', d['data_keys'].copy())
            for k in discard_fields:
                del d['data_keys'][k]
            d['data_keys'].update(all_extra_dk)

            if not len(d['data_keys']) and not len(all_extra_data):
                continue

            yield 'descriptor', d
            ev_gen = self.mds.get_events_generator(d)
            if fill:
                ev_gen = self.fill_event_stream(
                    ev_gen, d, in_place=True, **kwargs)
            for ev in ev_gen:
                event_data = ev.data  # cache for perf
                event_timestamps = ev.timestamps
                event_data.update(all_extra_data)
                event_timestamps.update(all_extra_ts)
                for field in discard_fields:
                    del event_data[field]
                    del event_timestamps[field]
                if not event_data:
                    # Skip events that are now empty because they had no
                    # applicable fields.
                    continue

                yield 'event', ev

        yield 'stop', header.stop

    def table_given_header(self, header, stream_name,
                           fields=None,
                           fill=False, convert_times=True, timezone=None,
                           handler_registry=None, handler_overrides=None,
                           localize_times=True):
        """
        Make a table (pandas.DataFrame) from given header.

        Parameters
        ----------
        header : Header
            The header to fetch the table for
        fields : list, optional
            whitelist of field names of interest; if None, all are returned
        stream_name : string, optional
            Get data from a single "event stream." To obtain one comprehensive
            table with all streams, use `stream_name=ALL` (where `ALL` is a
            sentinel class defined in this module). The default name is
            'primary', but if no event stream with that name is found, the
            default reverts to `ALL` (for backward-compatibility).
        fill : bool, optional
            Whether externally-stored data should be filled in. Defaults to False.
        convert_times : bool, optional
            Whether to convert times from float (seconds since 1970) to
            numpy datetime64, using pandas. True by default, returns naive
            datetime64 objects in UTC
        timezone : str, optional
            e.g., 'US/Eastern'
        handler_registry : dict, optional
            mapping filestore specs (strings) to handlers (callable classes)
        handler_overrides : dict, optional
            mapping data keys (strings) to handlers (callable classes)
        localize_times : bool, optional
            If the times should be localized to the 'local' time zone.  If
            True (the default) the time stamps are converted to the localtime
            zone (as configure in mds).

            This is problematic for several reasons:

              - apparent gaps or duplicate times around DST transitions
              - incompatibility with every other time stamp (which is in UTC)

            however, this makes the dataframe repr look nicer

            This implies convert_times.

            Defaults to True to preserve back-compatibility.

        Returns
        -------
        table : pandas.DataFrame
        """

        no_fields_filter = False
        if fields is None:
            no_fields_filter = True
            fields = []
        fields = set(fields)

        comp_re = _compile_re(fields)

        descs = self.descriptors_given_stream(header, stream_name)

        start = header['start']
        stop = header.get('stop', {})
        descs = self.descriptors_given_stream(header, stream_name)
        dfs = []
        for d in descs:
            (all_extra_dk, all_extra_data,
             all_extra_ts, discard_fields) = _extract_extra_data(
                 start, stop, d, fields, comp_re, no_fields_filter)

            payload = self.mds.get_events_table(d)
            _, data, seq_nums, times, uids, timestamps = payload
            df = pd.DataFrame(index=seq_nums)
            # if converting to datetime64 (in utc or 'local' tz)
            if convert_times or localize_times:
                times = pd.to_datetime(times, unit='s')
            # make sure this is a series
            times = pd.Series(times, index=seq_nums)

            # if localizing to 'local' time
            if localize_times:
                times = (times
                         .dt.tz_localize('UTC')     # first make tz aware
                         .dt.tz_convert(timezone)   # convert to 'local'
                         .dt.tz_localize(None)      # make naive again
                         )

            df['time'] = times
            for field, values in six.iteritems(data):
                if field in discard_fields:
                    logger.debug('Discarding field %s', field)
                    continue
                df[field] = values
            if list(df.columns) == ['time']:
                # no content
                continue
            if fill:
                df = self.fill_table(df, d, in_place=True,
                                     handler_registry=handler_registry,
                                     handler_overrides=handler_overrides)

            for field, v in all_extra_data:
                df[field] = v

            dfs.append(df)

        if dfs:
            return pd.concat(dfs)
        else:
            # edge case: no data
            return pd.DataFrame()

    def fill_event(self, ev, in_place=False, fields=None,
                   handler_registry=None, handler_overrides=None):

        external_map = _external_keys(ev.descriptor)

        if fields is None:
            fields = set(k for k, v in external_map.items() if v is not None)

        if not in_place:
            ev = ev.copy()
            # reach in and cheat >:)
            dict.__setitem__(ev, 'data', ev['data'].copy())

        data = ev['data']

        # fast path with no by key name overrides
        if not handler_overrides:
            with self.fs.handler_context(handler_registry):
                for k in fields:
                    data[k] = self.fs.get_datum(data[k])
        else:
            mock_registries = {dk: defaultdict(lambda: handler)
                               for dk, handler in handler_overrides.items()}

            for k in fields:
                with self.fs.handler_context(
                        mock_registries.get(k, handler_registry)):
                    data[k] = self.fs.get_datum(data[k])

        return ev

    def fill_event_stream(self, ev_gen, d, in_place=False, fields=None,
                          handler_registry=None,
                          handler_overrides=None):

        external_map = _external_keys(d)

        if fields is None:
            fields = set(k for k, v in external_map.items() if v is not None)

        # fast path with no by key name overrides
        if not handler_overrides:
            with self.fs.handler_context(handler_registry):
                for ev in ev_gen:
                    if not in_place:
                        ev = ev.copy()
                        # reach in and cheat >:)
                        dict.__setitem__(ev, 'data', ev['data'].copy())

                    data = ev['data']

                    for k in fields:
                        data[k] = self.fs.get_datum(data[k])

                    yield ev
        else:
            for ev in ev_gen:
                yield self.fill_event(ev, in_place, fields,
                                      handler_registry,
                                      handler_overrides)

    def fill_table(self, tab, descriptor, in_place=False,
                   handler_registry=None, handler_overrides=None):
        external_map = _external_keys(descriptor)
        if handler_overrides is None:
            handler_overrides = {}
        mock_registries = {data_key: defaultdict(lambda: handler)
                           for data_key, handler in
                           handler_overrides.items()}
        if not in_place:
            tab = tab.copy()
        for field in tab.columns:
            if external_map.get(field) is not None:
                logger.debug('filling data for %s', field)
                # TODO someday we will have bulk get_datum in FS
                if handler_overrides:
                    hr = mock_registries.get(field, handler_registry)
                else:
                    hr = handler_registry
                with self.fs.handler_context(hr) as _fs:
                    values = [_fs.get_datum(value)
                              for value in tab[field]]
                tab[field] = values
        return tab


def _extract_extra_data(start, stop, d, fields, comp_re,
                        no_fields_filter):
    if fields:
        event_fields = set(d['data_keys'])
        selected_fields = set(filter(comp_re.match, event_fields))
        discard_fields = event_fields - selected_fields
    else:
        discard_fields = set()
        selected_fields = set(d['data_keys'])

    objs_config = d.get('configuration', {}).values()
    config_data = merge(obj_conf['data'] for obj_conf in objs_config)
    config_ts = merge(obj_conf['timestamps']
                      for obj_conf in objs_config)
    all_extra_data = {}
    all_extra_ts = {}
    all_extra_dk = {}
    if not no_fields_filter:
        for dt, ts in [(config_data, config_ts),
                       (start, defaultdict(lambda: start['time'])),
                       (stop, defaultdict(lambda: stop['time']))]:
            # Look in the descriptor, then start, then stop.
            l_dk, l_data, l_ts = _project_header_data(
                dt, ts, selected_fields, comp_re)
            all_extra_data.update(l_data)
            all_extra_ts.update(l_ts)
            selected_fields.update(l_data)
            all_extra_dk.update(l_dk)

    return (all_extra_dk, all_extra_data, all_extra_ts,
            discard_fields)
