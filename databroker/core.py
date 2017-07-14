from __future__ import print_function
import six  # noqa
from collections import defaultdict, deque
from datetime import datetime
import pytz
import doct
from pims import FramesSequence, Frame
import logging
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
                                       for es in self.db.event_sources),
                                      [])
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

    @property
    def stream_names(self):
        return self.db.stream_names_given_header(self)

    def fields(self, stream_name=ALL):
        fields = set()
        for es in self.db.event_sources:
            fields.update(es.fields_given_header(header=self))
        return fields

    def config_data(self, obj_name):
        """
        Extract device configuration data from Event Descriptors.

        This refers to the data obtained from ``device.read_configuration()``.

        See example below. The result is structed as a [...deep breath...]
        dictionary of lists of dictionaries because:

        * The device might have been read in multiple event streams
          ('primary', 'baseline', etc.). Each stream name is a key in the
          outer dictionary.
        * The configuration is typically read once per event stream, but in
          general may be read multiple times if the configuration is changed
          mid-stream. Thus, a list is needed.
        * Each device typically produces multiple configuration fields
          ('exposure_time', 'period', etc.). These are the keys of the inner
          dictionary.

        Parameters
        ----------
        obj_name : string
            device name (as in ``device.name``)

        Returns
        -------
        result : dict
            mapping each stream name (such as 'primary' or 'baseline') to a
            list of data dictionaries

        Example
        --------

        Get the device configuration recorded for the object det.

        >>> h.config_data('det')
        {'primary': [{'exposure_time': 1.0}]}
        >>> exp_time = h.config_data('det')['primary'][0]['exposure_time']
        """
        result = defaultdict(list)
        for d in sorted(self.descriptors, key=lambda d: d['time']):
            config = d['configuration'].get(obj_name)
            if config:
                result[d['name']].append(config['data'])
        return dict(result)  # strip off defaultdict behavior

    def stream(self, stream_name=ALL, fill=False, **kwargs):
        gen = self.db.get_documents(self, stream_name=stream_name,
                                    fill=fill, **kwargs)
        for payload in gen:
            yield payload

    def table(self, stream_name='primary', fill=False, fields=None,
              timezone=None, convert_times=True, localize_times=True,
              **kwargs):
        '''
        Make a table (pandas.DataFrame) from given run(s).

        Parameters
        ----------
        headers : Header or iterable of Headers
            The headers to fetch the events for
        fields : list, optional
            whitelist of field names of interest; if None, all are returned
        stream_name : string, optional
            Get data from a single "event stream." To obtain one comprehensive
            table with all streams, use ``stream_name=ALL`` (where ``ALL`` is a
            sentinel class defined in this module). The default name is
            'primary', but if no event stream with that name is found, the
            default reverts to ``ALL`` (for backward-compatibility).
        fill : bool, optional
            Whether externally-stored data should be filled in.
            Defaults to True
        convert_times : bool, optional
            Whether to convert times from float (seconds since 1970) to
            numpy datetime64, using pandas. True by default.
        timezone : str, optional
            e.g., 'US/Eastern'; if None, use metadatastore configuration in
            `self.mds.config['timezone']`
        '''
        return self.db.get_table(self, fields=fields,
                                 stream_name=stream_name, fill=fill,
                                 timezone=timezone,
                                 convert_times=convert_times,
                                 localize_times=localize_times,
                                 **kwargs)


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
        if name is not None and name != descriptor.get('name', 'primary'):
            continue
        for field in descriptor['data_keys'].keys():
            fields.add(field)
    return fields


def get_images(db, headers, name, handler_registry=None,
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
    return Images(db.mds, db.es, db.fs, headers, name, handler_registry,
                  handler_override)


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
                return super(DocBuffer, self).__getitem__(key)

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


def format_time(search_dict, tz):
    """Helper function to format the time arguments in a search dict

    Expects 'start_time' and 'stop_time'

    ..warning: Does in-place mutation of the search_dict
    """
    time_dict = {}
    start_time = search_dict.pop('start_time', None)
    stop_time = search_dict.pop('stop_time', None)
    if start_time:
        time_dict['$gte'] = _normalize_human_friendly_time(start_time, tz)
    if stop_time:
        time_dict['$lte'] = _normalize_human_friendly_time(stop_time, tz)
    if time_dict:
        search_dict['time'] = time_dict


# human friendly timestamp formats we'll parse
_TS_FORMATS = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',  # these 2 are not as originally doc'd,
    '%Y-%m-%d %H',     # but match previous pandas behavior
    '%Y-%m-%d',
    '%Y-%m',
    '%Y']

# build a tab indented, '-' bulleted list of supported formats
# to append to the parsing function docstring below
_doc_ts_formats = '\n'.join('\t- {}'.format(_) for _ in _TS_FORMATS)


def _normalize_human_friendly_time(val, tz):
    """Given one of :
    - string (in one of the formats below)
    - datetime (eg. datetime.now()), with or without tzinfo)
    - timestamp (eg. time.time())
    return a timestamp (seconds since jan 1 1970 UTC).

    Non string/datetime values are returned unaltered.
    Leading/trailing whitespace is stripped.
    Supported formats:
    {}
    """
    # {} is placeholder for formats; filled in after def...

    zone = pytz.timezone(tz)  # tz as datetime.tzinfo object
    epoch = pytz.UTC.localize(datetime(1970, 1, 1))
    check = True

    if isinstance(val, six.string_types):
        # unix 'date' cmd format '%a %b %d %H:%M:%S %Z %Y' works but
        # doesn't get TZ?

        # Could cleanup input a bit? remove leading/trailing [ :,-]?
        # Yes, leading/trailing whitespace to match pandas behavior...
        # Actually, pandas doesn't ignore trailing space, it assumes
        # the *current* month/day if they're missing and there's
        # trailing space, or the month is a single, non zero-padded digit.?!
        val = val.strip()

        for fmt in _TS_FORMATS:
            try:
                ts = datetime.strptime(val, fmt)
                break
            except ValueError:
                pass

        try:
            if isinstance(ts, datetime):
                val = ts
                check = False
            else:
                # what else could the type be here?
                raise TypeError('expected datetime,'
                                ' got {:r}'.format(ts))

        except NameError:
            raise ValueError('failed to parse time: ' + repr(val))

    if check and not isinstance(val, datetime):
        return val

    if val.tzinfo is None:
        # is_dst=None raises NonExistent and Ambiguous TimeErrors
        # when appropriate, same as pandas
        val = zone.localize(val, is_dst=None)

    return (val - epoch).total_seconds()


# fill in the placeholder we left in the previous docstring
_normalize_human_friendly_time.__doc__ = (
    _normalize_human_friendly_time.__doc__.format(_doc_ts_formats)
)
