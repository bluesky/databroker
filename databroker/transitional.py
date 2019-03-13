from collections.abc import Iterable
from collections import defaultdict
import pandas
from intake import Catalog
import re

# This triggers driver registration.
import intake_bluesky.core  # noqa
import intake_bluesky.mongo_layout1  # noqa

# Toolz and CyToolz have identical APIs -- same test suite, docstrings.
try:
    from cytoolz.dicttoolz import merge
except ImportError:
    from toolz.dicttoolz import merge

from .utils import ALL, get_fields, wrap_in_deprecated_doct, wrap_in_doct


class Broker:
    """
    This supports the original Broker API but implemented on intake.Catalog.
    """
    def __init__(self, uri, source, header_version=1):
        catalog = Catalog(str(uri))
        if source is not None:
            catalog = catalog[source]()
        self._catalog = catalog
        self._header_version = header_version
        self.prepare_hook = wrap_in_deprecated_doct
        self.aliases = {}

    @property
    def header_version(self):
        return self._header_version

    @property
    def _api_version_2(self):
        return self._catalog

    def __call__(self, text_search=None, **kwargs):
        data_key = kwargs.pop('data_key', None)
        return Results(self, self._catalog.search(kwargs),
                       data_key, self._header_version)

    def __getitem__(self, key):
        # If this came from a client, we might be getting '-1'.
        if not isinstance(key, str) and isinstance(key, Iterable):
            return [self[item] for item in key]
        entry = self._catalog[key]
        if self._header_version == 1:
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

    def __getattr__(self, key):
        try:
            query = self.aliases[key]
        except KeyError:
            raise AttributeError(key)
        if callable(query):
            query = query()
        return self(**query)


class Header:
    """
    This supports the original Header API but implemented on intake's Entry.
    """
    def __init__(self, entry, broker):
        self.start = entry.metadata['start']
        self.stop = entry.metadata['stop']
        self._entry = entry
        self._descriptors = None  # Fetch lazily in property.
        self.ext = None  # TODO
        self.db = broker

    @property
    def _api_version_2(self):
        return self._entry

    @property
    def descriptors(self):
        if self._descriptors is None:
            self._descriptors = []
            catalog = self._entry()
            for name, entry in catalog._entries.items():
                self._descriptors.extend(entry.metadata['descriptors'])
        return self._descriptors

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
        self._header_version = header_version

    def __iter__(self):
        # TODO Catalog.walk() fails. We should probably support Catalog.items().
        for uid, entry in self._catalog._entries.items():
            if self._header_version == 1:
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
