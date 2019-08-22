from collections.abc import Iterable
from collections import defaultdict
from datetime import datetime
import pandas
from intake import Catalog
import re
import warnings
import time
import humanize
import jinja2
from types import SimpleNamespace
import tzlocal

# This triggers driver registration.
import intake_bluesky.core  # noqa
import intake_bluesky.mongo_normalized  # noqa

# Toolz and CyToolz have identical APIs -- same test suite, docstrings.
try:
    from cytoolz.dicttoolz import merge
except ImportError:
    from toolz.dicttoolz import merge

from .utils import (ALL, format_time, get_fields, wrap_in_deprecated_doct,
                    wrap_in_doct)


class Registry:
    """
    An accessor that serves as a backward-compatible shim for Broker.reg
    """
    def __init__(self, filler):
        self._filler = filler

    @property
    def handler_reg(self):
        return self._filler.handler_registry

    def register_handler(self, key, handler, overwrite=False):
        if (not overwrite) and (key in self.handler_reg):
            if self.handler_reg[key] is handler:
                return
            raise self.DuplicateHandler(
                "You are trying to register a second handler "
                "for spec {}, {}".format(key, self))

        self.deregister_handler(key)
        self.handler_reg[key] = handler

    def deregister_handler(self, key):
        handler = self.handler_reg.pop(key, None)
        # TODO Filler should support explicit de-registration that clears
        # the relevant caches.


class Broker:
    """
    This supports the original Broker API but implemented on intake.Catalog.
    """
    def __init__(self, catalog, header_version=1, external_fetchers=None):
        self._catalog = catalog
        self.header_version = header_version
        self.external_fetchers = external_fetchers or {}
        self.prepare_hook = wrap_in_deprecated_doct
        self.aliases = {}
        self.filters = {}
        self.reg = Registry(catalog.filler)

    @classmethod
    def from_config(cls, config, auto_register=True, name=None):
        # Provide a way to force v0 Broker as fall-back.
        if config.get('api_version') == 0:
            from .v0 import Broker
            return Broker.from_config(config, auto_register, name)
        if 'uri' in config:
            from .v2 import Broker
            catalog = Broker(str(uri))
            if config.get('source') is not None:
                catalog = catalog[source]()
            return cls(catalog)
        elif 'metadatastore' in config:
            return parse_v0_config(config)
        else:
            # TODO Parse old-style config by extracing Mongo connection info.
            raise NotImplementedError("Unable to parse config.")

    @property
    def v2(self):
        "Accessor to the version 2 API."
        return self._catalog

    def fetch_external(self, start, stop):
        return {k: func(start, stop) for
                k, func in self.external_fetchers.items()}

    def __call__(self, text_search=None, **kwargs):
        data_key = kwargs.pop('data_key', None)
        tz = tzlocal.get_localzone().zone
        if self.filters:
            filters = self.filters.copy()
            format_time(filters, tz)  # mutates in place
            catalog = self._catalog.search(filters)
        else:
            catalog = self._catalog
        if text_search:
            kwargs.update({'$text': {'$search': text_search}})
        format_time(kwargs, tz)  # mutates in place
        return Results(self, catalog.search(kwargs),
                       data_key, self.header_version)

    def __getitem__(self, key):
        # If this came from a client, we might be getting '-1'.
        if not isinstance(key, str) and isinstance(key, Iterable):
            return [self[item] for item in key]
        if isinstance(key, slice):
            if key.start is not None and key.start > -1:
                raise ValueError("slice.start must be negative. You gave me "
                                "key=%s The offending part is key.start=%s"
                                % (key, key.start))
            if key.stop is not None and key.stop > 0:
                raise ValueError("slice.stop must be <= 0. You gave me key=%s. "
                                "The offending part is key.stop = %s"
                                % (key, key.stop))
            if key.start is None:
                raise ValueError("slice.start cannot be None because we do not "
                                "support slicing infinitely into the past; "
                                "the size of the result is non-deterministic "
                                "and could become too large.")
            return [self[index]
                    for index in reversed(range(key.start, key.stop or 0, key.step or 1))]
        entry = self._catalog[key]
        if self.header_version == 1:
            return Header(entry, self)
        else:
            return entry

    get_fields = staticmethod(get_fields)

    def get_documents(self,
                      headers, stream_name=ALL, fields=None, fill=False,
                      handler_registry=None):
        """
        Get all documents from one or more runs.

        Parameters
        ----------
        headers : Header or iterable of Headers
            The headers to fetch the events for

        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is `ALL` which yields documents for all streams.

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        handler_registry : dict, optional
            mapping asset pecs (strings) to handlers (callable classes)

        Yields
        ------
        name : str
            The name of the kind of document

        doc : dict
            The payload, may be RunStart, RunStop, EventDescriptor, or Event.

        Raises
        ------
        ValueError if any key in `fields` is not in at least one descriptor
        pre header.
        """
        headers = _ensure_list(headers)

        no_fields_filter = False
        if fields is None:
            no_fields_filter = True
            fields = []
        fields = set(fields)

        comp_re = _compile_re(fields)

        for header in headers:
            uid = header.start['uid']
            descs = header.descriptors

            descriptors = set()
            per_desc_discards = {}
            per_desc_extra_data = {}
            per_desc_extra_ts = {}
            for d in descs:
                (all_extra_dk, all_extra_data,
                all_extra_ts, discard_fields) = _extract_extra_data(
                    header.start, header.stop, d, fields, comp_re,
                    no_fields_filter)

                per_desc_discards[d['uid']] = discard_fields
                per_desc_extra_data[d['uid']] = all_extra_data
                per_desc_extra_ts[d['uid']] = all_extra_ts

                d = d.copy()
                dict.__setitem__(d, 'data_keys', d['data_keys'].copy())
                for k in discard_fields:
                    del d['data_keys'][k]
                d['data_keys'].update(all_extra_dk)

                if not len(d['data_keys']) and not len(all_extra_data):
                    continue

            for name, doc in self._catalog[uid].read_canonical():
                if stream_name is not ALL:
                    # Filter by stream_name.
                    if name == 'descriptor':
                        if doc.get('name', 'primary') == stream_name:
                            descriptors.add(doc['uid'])
                        else:
                            continue
                    elif name == 'event':
                        if doc['descriptor'] not in descriptors:
                            continue
                        event_data = doc['data']  # cache for perf
                        desc = doc['descriptor']
                        event_timestamps = doc['timestamps']
                        event_data.update(per_desc_extra_data[desc])
                        event_timestamps.update(per_desc_extra_ts[desc])
                        discard_fields = per_desc_discards[desc]
                        for field in discard_fields:
                            del event_data[field]
                            del event_timestamps[field]
                        if not event_data:
                            # Skip events that are now empty because they had no
                            # applicable fields.
                            continue
                yield name, self.prepare_hook(name, doc)

    def get_events(self,
                   headers, stream_name='primary', fields=None, fill=False,
                   handler_registry=None):
        """
        Get Event documents from one or more runs.

        Parameters
        ----------
        headers : Header or iterable of Headers
            The headers to fetch the events for

        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is 'primary'

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        handler_registry : dict, optional
            mapping asset specs (strings) to handlers (callable classes)

        Yields
        ------
        event : Event
            The event, optionally with non-scalar data filled in

        Raises
        ------
        ValueError if any key in `fields` is not in at least one descriptor
        pre header.
        """
        for name, doc in self.get_documents(headers,
                                            fields=fields,
                                            stream_name=stream_name,
                                            fill=fill,
                                            handler_registry=handler_registry):
            if name == 'event':
                yield doc

    def get_table(self,
                  headers, stream_name='primary', fields=None, fill=False,
                  handler_registry=None,
                  convert_times=True, timezone=None, localize_times=True):
        """
        Load the data from one or more runs as a table (``pandas.DataFrame``).

        Parameters
        ----------
        headers : Header or iterable of Headers
            The headers to fetch the events for

        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is 'primary'

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        handler_registry : dict, optional
            mapping filestore specs (strings) to handlers (callable classes)

        convert_times : bool, optional
            Whether to convert times from float (seconds since 1970) to
            numpy datetime64, using pandas. True by default.

        timezone : str, optional
            e.g., 'US/Eastern'; if None, use metadatastore configuration in
            `self.mds.config['timezone']`

        handler_registry : dict, optional
            mapping asset specs (strings) to handlers (callable classes)

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
        headers = _ensure_list(headers)
        dfs = []
        for header in headers:
            uid = header.start['uid']
            df = (self._catalog[uid][stream_name].read()
                  .to_dataframe()
                  .set_index('seq_num'))
            dfs.append(df)
        return pandas.concat(dfs)

    def alias(self, key, **query):
        """
        Create an alias for a query.

        Parameters
        ----------
        key : string
            must be a valid Python identifier
        query :
            keyword argument comprising a query

        Examples
        --------
        Define an alias that searches for headers with purpose='calibration'.

        >>> db.alias('cal', purpose='calibration')

        Use it.

        >>> headers = db.cal  # -> db(purpose='calibration')

        Review defined aliases.

        >>> db.aliases
        {'cal': {'purpose': 'calibration'}}
        """
        if hasattr(self, key) and key not in self.aliases:
            raise ValueError("'%s' is not a legal alias." % key)
        self.aliases[key] = query

    def dynamic_alias(self, key, func):
        """
        Create an alias for a "dynamic" query, a function that returns a query.

        Parameters
        ----------
        key : string
            must be a valid Python identifier
        func : callable
            When called with no arguments, must return a dict that is a valid
            query.

        Examples
        --------
        Define an alias to get headers from the last 24 hours.

        >>> import time
        >>> db.dynamic_alias('today',
        ...                  lambda: {'since': time.time() - 24*60*60})

        Use it.

        >>> headers = db.today

        Define an alias to get headers with the 'user' field in metadata
        matches the current logged-in user.

        >>> import getpass
        >>> db.dynamic_alias('mine', lambda: {'user': getpass.getuser()})

        Use it

        >>> headers = db.mine
        """
        if hasattr(self, key) and key not in self.aliases:
            raise ValueError("'%s' is not a legal alias." % key)
        self.aliases[key] = func

    def add_filter(self, **kwargs):
        """
        Add query to the list of 'filter' queries.

        Any query passed to ``db.add_filter()`` is stashed and "AND-ed" with
        all future queries.

        ``db.add_filter(**kwargs)`` is just a convenient way to spell
        ``db.filters.update(**kwargs)``.

        Examples
        --------
        Filter all searches to restrict results to a specific user after a
        March 2017.

        >>> db.add_filter(user='Dan')
        >>> db.add_filter(since='2017-3')

        The following query is equivalent to
        ``db(user='Dan', plan_name='scan')``.

        >>> db(plan_name='scan')

        Review current filters.

        >>> db.filters
        {'user': 'Dan', 'since': '2017-3'}

        Clear filters.

        >>> db.clear_filters()

        See Also
        --------
        :meth:`Broker.clear_filters`

        """
        self.filters.update(**kwargs)

    def clear_filters(self, **kwargs):
        """
        Clear all 'filter' queries.

        Filter queries are combined with every given query using '$and',
        acting as a filter to restrict the results.

        ``Broker.clear_filters()`` is just a convenient way to spell
        ``Broker.filters.clear()``.

        See Also
        --------
        :meth:`Broker.add_filter`
        """
        self.filters.clear()

    def __getattr__(self, key):
        try:
            query = self.aliases[key]
        except KeyError:
            raise AttributeError(key)
        if callable(query):
            query = query()
        return self(**query)

    def restream(self, headers, fields=None, fill=False):
        """
        Get all Documents from given run(s).

        This output can be used as a drop-in replacement for the output of the
        bluesky Run Engine.

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

        Examples
        --------
        >>> def f(name, doc):
        ...     # do something
        ...
        >>> h = db[-1]  # most recent header
        >>> for name, doc in restream(h):
        ...     f(name, doc)

        See Also
        --------
        :meth:`Broker.process`
        """
        for payload in self.get_documents(headers, fields=fields, fill=fill):
            yield payload

    stream = restream  # compat

    def process(self, headers, func, fields=None, fill=False):
        """
        Pass all the documents from one or more runs into a callback.

        This output can be used as a drop-in replacement for the output of the
        bluesky Run Engine.

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

        Examples
        --------
        >>> def f(name, doc):
        ...     # do something
        ...
        >>> h = db[-1]  # most recent header
        >>> process(h, f)

        See Also
        --------
        :meth:`Broker.restream`
        """
        for name, doc in self.get_documents(headers, fields=fields, fill=fill):
            func(name, doc)


class Header:
    """
    This supports the original Header API but implemented on intake's Entry.
    """
    def __init__(self, entry, broker):
        self._entry = entry
        self.__data_source = None
        self.db = broker
        self.ext = None  # TODO
        self._start = entry.describe()['metadata']['start']
        self._stop = entry.describe()['metadata']['stop']
        self.ext = SimpleNamespace(
            **self.db.fetch_external(self.start, self.stop))

    @property
    def _data_source(self):
        if self.__data_source is None:
            self.__data_source = self._entry.get()
        return self.__data_source

    @property
    def start(self):
        return self.db.prepare_hook('start', self._start)

    @property
    def stop(self):
        if self._stop is None:
            self._stop = self._entry.describe()['metadata']['stop'] or {}
        return self.db.prepare_hook('stop', self._stop)

    def __eq__(self, other):
        return self.start == other.start

    @property
    def _api_version_2(self):
        return self._entry

    @property
    def descriptors(self):
        descriptors = []
        for name, entry in self._data_source._entries.items():
            descriptors.extend(entry.describe()['metadata']['descriptors'])
        return [self.db.prepare_hook('descriptor', doc)
                for doc in descriptors]

    @property
    def stream_names(self):
        catalog = self._entry()
        return list(catalog)

    # These methods mock part of the dict interface. It has been proposed that
    # we might remove them for 1.0.

    def __getitem__(self, k):
        if k in ('start', 'descriptors', 'stop', 'ext'):
            return getattr(self, k)
        else:
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
        for k in ('start', 'descriptors', 'stop', 'ext'):
            yield k

    def __iter__(self):
        return self.keys()

    def table(self, stream_name='primary', fields=None, fill=False,
              timezone=None, convert_times=True, localize_times=True):
        '''
        Load the data from one event stream as a table (``pandas.DataFrame``).

        Parameters
        ----------
        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is 'primary'

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        handler_registry : dict, optional
            mapping filestore specs (strings) to handlers (callable classes)

        convert_times : bool, optional
            Whether to convert times from float (seconds since 1970) to
            numpy datetime64, using pandas. True by default.

        timezone : str, optional
            e.g., 'US/Eastern'; if None, use metadatastore configuration in
            `self.mds.config['timezone']`

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

        Examples
        --------
        Load the 'primary' data stream from the most recent run into a table.

        >>> h = db[-1]
        >>> h.table()

        This is equivalent. (The default stream_name is 'primary'.)

        >>> h.table(stream_name='primary')
                                    time intensity
        0  2017-07-16 12:12:37.239582345       102
        1  2017-07-16 12:12:39.958385283       103

        Load the 'baseline' data stream.

        >>> h.table(stream_name='baseline')
                                    time temperature
        0  2017-07-16 12:12:35.128515999         273
        1  2017-07-16 12:12:40.128515999         274
        '''
        return self.db.get_table(self, fields=fields,
                                 stream_name=stream_name, fill=fill,
                                 timezone=timezone,
                                 convert_times=convert_times,
                                 localize_times=localize_times)


    def documents(self, stream_name=ALL, fields=None, fill=False):
        """
        Load all documents from the run.

        This is a generator the yields ``(name, doc)``.

        Parameters
        ----------
        stream_name : string or ``ALL``, optional
            Filter results by stream name (e.g., 'primary', 'baseline'). The
            default, ``ALL``, combines results from all streams.
        fill : bool, optional
            Whether externally-stored data should be filled in. False by
            default.

        Yields
        ------
        name, doc : (string, dict)

        Examples
        --------
        Loop through the documents from a run.

        >>> h = db[-1]
        >>> for name, doc in h.documents():
        ...     # do something
        """
        gen = self.db.get_documents(self, fields=fields,
                                    stream_name=stream_name,
                                    fill=fill)
        for payload in gen:
            yield payload

    def data(self, field, stream_name='primary', fill=True):
        """
        Extract data for one field. This is convenient for loading image data.

        Parameters
        ----------
        field : string
            such as 'image' or 'intensity'

        stream_name : string, optional
            Get data from a single "event stream." Default is 'primary'

        fill : bool, optional
             If the data should be filled.

        Yields
        ------
        data
        """
        if fill:
            fill = {field}
        for event in self.events(stream_name=stream_name,
                                 fields=[field],
                                 fill=fill):
            yield event['data'][field]

    def stream(self, *args, **kwargs):
        warnings.warn(
            "The 'stream' method been renamed to 'documents'. The old name "
            "will be removed in the future.")
        for payload in self.documents(*args, **kwargs):
            yield payload

    def fields(self, stream_name=ALL):
        """
        Return the names of the fields ('data keys') in this run.

        Parameters
        ----------
        stream_name : string or ``ALL``, optional
            Filter results by stream name (e.g., 'primary', 'baseline'). The
            default, ``ALL``, combines results from all streams.

        Returns
        -------
        fields : set

        Examples
        --------
        Load the most recent run and list its fields.

        >>> h = db[-1]
        >>> h.fields()
        {'eiger_stats1_total', 'eiger_image'}

        See Also
        --------
        :meth:`Header.devices`
        """
        fields = set()
        for descriptor in self.descriptors:
            if stream_name is ALL or descriptor.get('name') == stream_name:
                fields.update(descriptor['data_keys'])
        return fields

    def config_data(self, device_name):
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
        device_name : string
            device name (originally obtained from the ``name`` attribute of
            some readable Device)

        Returns
        -------
        result : dict
            mapping each stream name (such as 'primary' or 'baseline') to a
            list of data dictionaries

        Examples
        --------
        Get the device configuration recorded for the device named 'eiger'.

        >>> h.config_data('eiger')
        {'primary': [{'exposure_time': 1.0}]}

        Assign the exposure time to a variable.

        >>> exp_time = h.config_data('eiger')['primary'][0]['exposure_time']

        How did we know that ``'eiger'`` was a valid argument? We can query for
        the complete list of device names:

        >>> h.devices()
        {'eiger', 'cs700'}
        """
        result = defaultdict(list)
        for d in sorted(self.descriptors, key=lambda d: d['time']):
            config = d['configuration'].get(device_name)
            if config:
                result[d.get('name')].append(config['data'])
        return dict(result)  # strip off defaultdict behavior

    def events(self, stream_name='primary', fields=None, fill=False):
        """
        Load all Event documents from one event stream.

        This is a generator the yields Event documents.

        Parameters
        ----------
        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is 'primary'

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        Yields
        ------
        doc : dict

        Examples
        --------
        Loop through the Event documents from a run. This is 'lazy', meaning
        that only one Event at a time is loaded into memory.

        >>> h = db[-1]
        >>> for event in h.events():
        ...    # do something

        List the Events documents from a run, loading them all into memory at
        once.

        >>> events = list(h.events())
        """
        ev_gen = self.db.get_events([self], stream_name=stream_name,
                                    fields=fields, fill=fill)
        for ev in ev_gen:
            yield ev

    def _repr_html_(self):
        env = jinja2.Environment()
        env.filters['human_time'] = _pretty_print_time
        template = env.from_string(_HTML_TEMPLATE)
        return template.render(document=self)


class Results:
    """
    Iterable object encapsulating a results set of Headers

    Parameters
    ----------
    catalog : Catalog
        search results
    data_key : string or None
        Special query parameter that filters results
    """
    def __init__(self, broker, catalog, data_key, header_version):
        self._broker = broker
        self._catalog = catalog
        self._data_key = data_key
        self.header_version = header_version

    def __iter__(self):
        # TODO Catalog.walk() fails. We should probably support Catalog.items().
        for uid, entry in self._catalog._entries.items():
            if self.header_version == 1:
                header = Header(entry, self._broker)
            else:
                header = entry
            if self._data_key is None:
                yield header
            else:
                # Only include this header in the result if `data_key` is found
                # in one of its descriptors' data_keys.
                for descriptor in header.descriptors:
                    if self._data_key in descriptor['data_keys']:
                        yield header
                        break


def _ensure_list(headers):
    try:
        headers.items()
    except AttributeError:
        return headers
    else:
        return [headers]


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


def _extract_extra_data(start, stop, d, fields, comp_re,
                        no_fields_filter):
    def _project_header_data(source_data, source_ts,
                             selected_fields, comp_re):
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


_HTML_TEMPLATE = """
{% macro rtable(doc, cap) -%}
<table>
<caption> {{ cap }} </caption>
{%- for key, value in doc | dictsort recursive -%}
  <tr>
    <th> {{ key }} </th>
    <td>
      {%- if value.items -%}
        <table>
          {{ loop(value | dictsort) }}
        </table>
      {%- elif value is iterable and value is not string -%}
        <table>
          {%- set outer_loop = loop -%}
          {%- for stuff in value  -%}
            {%- if stuff.items -%}
               {{ outer_loop(stuff | dictsort) }}
            {%- else -%}
              <tr><td>{{ stuff }}</td></tr>
            {%- endif -%}
          {%- endfor -%}
        </table>
      {%- else -%}
        {%- if key == 'time' -%}
          {{ value | human_time }}
        {%- else -%}
          {{ value }}
        {%- endif -%}
      {%- endif -%}
    </td>
  </tr>
{%- endfor -%}
</table>
{%- endmacro %}

<table>
  <tr>
    <td>{{ rtable(document.start, 'Start') }}</td>
  </tr
  <tr>
    <td>{{ rtable(document.stop, 'Stop') }}</td>
  </tr>
  <tr>
  <td>
      <table>
      <caption>Descriptors</caption>
         {%- for d in document.descriptors -%}
         <tr>
         <td> {{ rtable(d, d.get('name')) }} </td>
         </tr>
         {%- endfor -%}
      </table>
    </td>
</tr>
</table>
"""


def _pretty_print_time(timestamp):
    # timestamp needs to be a float or fromtimestamp() will barf
    timestamp = float(timestamp)
    dt = datetime.fromtimestamp(timestamp).isoformat()
    ago = humanize.naturaltime(time.time() - timestamp)
    return '{ago} ({date})'.format(ago=ago, date=dt)


def parse_v0_config(config):
"""
Parse v0 configuration file and obtain a uri and source.

Sample

```
description: 'CSX raw data'
metadatastore:
    module: 'databroker.headersource.mongo'
    class: 'MDS'
    config:
        host: 'xf23id-broker'
        port: 27017
        database: 'datastore2'
        timezone: 'US/Eastern'
        auth: False
assets:
    module: 'databroker.assets.mongo'
    class: 'Registry'
    config:
        host: 'xf23id-broker'
        port: 27017
        database: 'filestore'
```
"""

