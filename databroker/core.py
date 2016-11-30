from __future__ import print_function
import six  # noqa
from collections import defaultdict
from itertools import chain
import pandas as pd
import tzlocal
import doct as doc
import doct
from pims import FramesSequence, Frame
import logging
import numbers
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
    descriptors = attr.ib()
    stop = attr.ib(default=attr.Factory(dict))

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
        mds = db.mds

        if isinstance(run_start, six.string_types):
            run_start = mds.run_start_given_uid(run_start)
        run_start_uid = run_start['uid']

        try:
            run_stop = doct.ref_doc_to_uid(mds.stop_by_start(run_start_uid),
                                           'run_start')
        except mds.NoRunStop:
            run_stop = None

        try:
            ev_descs = [doct.ref_doc_to_uid(ev_desc, 'run_start')
                        for ev_desc in
                        mds.descriptors_by_start(run_start_uid)]
        except mds.NoEventDescriptors:
            ev_descs = []

        d = {'start': run_start, 'descriptors': ev_descs}
        if run_stop is not None:
            d['stop'] = run_stop
        h = cls(db, **d)
        return h

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
        return self._name, ret

    def __str__(self):
        return doct.vstr(self)

    def __len__(self):
        return 3

    def __iter__(self):
        return self.keys()


def get_events(headers, fields=None, stream_name=ALL, fill=False,
               handler_registry=None, handler_overrides=None, plugins=None,
               **kwargs):
    """
    Get Events from given run(s).

    Parameters
    ----------
    mds : MDSRO
    fs : FileStoreRO
    headers : Header or iterable of Headers
        The headers to fetch the events for
    fields : list, optional
        whitelist of field names of interest or regular expression;
        if None, all are returned
    stream_name : string, optional
        Get events from only one "event stream" with this name. Default value
        is special sentinel class, `ALL`, which gets all streams together.
    fill : bool, optional
        Whether externally-stored data should be filled in. Defaults to False.
    handler_registry : dict, optional
        mapping filestore specs (strings) to handlers (callable classes)
    handler_overrides : dict, optional
        mapping data keys (strings) to handlers (callable classes)
    plugins : dict or None, optional
        mapping keyword arguments (strings) to Plugins
    kwargs
        passed through to any plugins

    Yields
    ------
    event : Event
        The event, optionally with non-scalar data filled in

    Raises
    ------
    ValueError if any key in `fields` is not in at least one descriptor pre header.
    KeyError if a kwarg is passed without a corresponding plugin.
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

    no_fields_filter = False
    if fields is None:
        no_fields_filter = True
        fields = []
    fields = set(fields)
    _check_fields_exist(fields, headers)

    comp_re = _compile_re(fields)

    for k in kwargs:
        if k not in plugins:
            raise KeyError("No plugin was found to handle the keyword "
                           "argument %r" % k)

    for header in headers:
        mds = header.db.mds
        fs = header.db.fs
        # cache these attribute look-ups for performance
        start = header['start']
        stop = header.get('stop', {})
        for descriptor in header['descriptors']:
            descriptor_name = descriptor.get('name')
            if (stream_name is not ALL) and (stream_name != descriptor_name):
                continue
            objs_config = descriptor.get('configuration', {}).values()
            config_data = merge(obj_conf['data'] for obj_conf in objs_config)
            config_ts = merge(obj_conf['timestamps']
                              for obj_conf in objs_config)
            if fields:
                event_fields = set(descriptor['data_keys'])
                selected_fields = set(filter(comp_re.match, event_fields))
                discard_fields = event_fields - selected_fields
            else:
                discard_fields = set()
                selected_fields = set()

            all_extra_data = {}
            all_extra_ts = {}

            if not no_fields_filter:
                # Look in the descriptor, then start, then stop.
                config_data_fields = (set(filter(comp_re.match, config_data)) -
                                      selected_fields)
                for field in config_data_fields:
                    selected_fields.add(field)
                    all_extra_data[field] = config_data[field]
                    all_extra_ts[field] = config_ts[field]

                start_fields = (set(filter(comp_re.match, start)) -
                                selected_fields)
                for field in start_fields:
                    all_extra_data[field] = start[field]
                    all_extra_ts[field] = start['time']

                stop_fields = (set(filter(comp_re.match, stop)) -
                               selected_fields)
                for field in stop_fields:
                    all_extra_data[field] = stop[field]
                    all_extra_ts[field] = stop['time']

            for event in mds.get_events_generator(descriptor):
                event_data = event.data  # cache for perf
                event_timestamps = event.timestamps
                event_data.update(all_extra_data)
                event_timestamps.update(all_extra_ts)
                for field in discard_fields:
                    del event_data[field]
                    del event_timestamps[field]
                if not event_data:
                    # Skip events that are now empty because they had no
                    # applicable fields.
                    continue
                if fill:
                    fill_event(fs, event, handler_registry, handler_overrides)
                yield event
        # Now yield any events from plugins.
        for k, v in kwargs.items():
            for ev in plugins[k].get_events(header, v):
                yield ev


def get_table(mds, fs, headers, fields=None, stream_name='primary', fill=False,
              convert_times=True, timezone=None, handler_registry=None,
              handler_overrides=None, localize_times=True):
    """
    Make a table (pandas.DataFrame) from given run(s).

    Parameters
    ----------
    mds : MDSRO
    fs : FileStoreRO
    headers : Header or iterable of Headers
        The headers to fetch the events for
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
    if handler_registry is None:
        handler_registry = {}

    if fields is None:
        fields = []
    fields = set(fields)
    _check_fields_exist(fields, headers)

    dfs = []
    for header in headers:
        # cache these attribute look-ups for performance
        start = header['start']
        stop = header.get('stop', {})
        descriptors = header['descriptors']
        if stop is None:
            stop = {}

        # shim for back-compat with old data that has no 'primary' descriptor
        if not any(d for d in descriptors if d.get('name') == 'primary'):
            stream_name = ALL

        for descriptor in descriptors:
            descriptor_name = descriptor.get('name')
            if (stream_name is not ALL) and (stream_name != descriptor_name):
                continue
            is_external = _external_keys(descriptor)
            objs_config = descriptor.get('configuration', {}).values()
            config_data = merge(obj_conf['data'] for obj_conf in objs_config)
            discard_fields = set()
            extra_fields = set()
            if fields:
                event_fields = set(descriptor['data_keys'])
                discard_fields = event_fields - fields
                extra_fields = fields - event_fields
            payload = mds.get_events_table(descriptor)
            descriptor, data, seq_nums, times, uids, timestamps = payload
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
            for field in df.columns:
                if is_external.get(field) is not None and fill:
                    logger.debug('filling data for %s', field)
                    # TODO someday we will have bulk get_datum in FS
                    datum_uids = df[field]
                    if field not in handler_overrides:
                        with fs.handler_context(handler_registry) as _fs:
                            values = [_fs.get_datum(value)
                                      for value in datum_uids]
                    else:
                        handler = handler_overrides[field]
                        mock_registry = defaultdict(lambda: handler)
                        with fs.handler_context(mock_registry) as _fs:
                            values = [_fs.get_datum(value)
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


def restream(mds, fs, headers, fields=None, fill=False):
    """
    Get all Documents from given run(s).

    Parameters
    ----------
    mds : MDSRO
    fs : FileStoreRO
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
        for event in get_events(header, fields=fields, fill=fill):
            yield 'event', event
        yield 'stop', header['stop']


stream = restream  # compat


def process(mds, fs, headers, func, fields=None, fill=False):
    """
    Get all Documents from given run to a callback.

    Parameters
    ----------
    mds : MDSRO
    fs : FileStoreRO
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
    for name, doc in restream(mds, fs, headers, fields, fill):
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
    def __init__(self, mds, fs, headers, name, handler_registry=None,
                 handler_override=None):
        """
        Load images from a detector for given Header(s).

        Parameters
        ----------
        fs : FileStoreRO
        headers : Header or list of Headers
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
        self.fs = fs
        events = get_events(headers, [name], fill=False)
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
        data_keys = descriptor.data_keys
        ek = {k: v.get('external', None) for k, v in data_keys.items()}
        _cache[descriptor['uid']] = ek
    return ek
