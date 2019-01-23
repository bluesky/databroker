from __future__ import print_function
import six  # noqa
from collections import defaultdict
from itertools import chain
import pandas as pd
import logging
import boltons.cacheutils
import heapq
import re
from ..utils import ALL

# Toolz and CyToolz have identical APIs -- same test suite, docstrings.
try:
    from cytoolz.dicttoolz import merge
except ImportError:
    from toolz.dicttoolz import merge

logger = logging.getLogger(__name__)


def interlace_gens(*gens):
    """Take generators and interlace their results by timestamp

    Parameters
    ----------
    gens : generators
        Generators of (name, dict) pairs where the dict contains a 'time'
        key.

    Yields
    -------
    val : dict
        The next document in time order

    """
    iters = [iter(g) for g in gens]
    heap = []

    def safe_next(itr, indx):
        try:
            val = next(itr)
        except StopIteration:
            return
        heapq.heappush(heap, (val['time'], indx, val, itr))

    for j, itr in enumerate(iters):
        safe_next(itr, j)
    while heap:
        _, j, val, itr = heapq.heappop(heap)
        yield val
        safe_next(itr, j)


class EventSourceShim(object):
    '''Shim class to turn a mds object into a EventSource

    This will presumably be deleted if this API makes it's way back down
    into the implementations
    '''

    @property
    def name(self):
        return 'mds'

    @property
    def NoEventDescriptors(self):
        return self.mds.NoEventDescriptors

    def __init__(self, mds, fs):
        self.mds = mds
        self.fs = fs

    def insert(self, name, doc):
        return self.mds.insert(name, doc)

    def stream_names_given_header(self, header):
        return set(d.get('name', 'primary') for d in
                   self.descriptors_given_header(header))

    def fields_given_header(self, header, stream_name=ALL):
        fields = set()
        for d in self.descriptors_given_header(header,
                                               stream_name=stream_name):
            fields.update(d['data_keys'])
        return fields

    def descriptors_given_header(self, header, stream_name=ALL):
        try:
            return [d for d in
                    self.mds.descriptors_by_start(header['start']['uid'])
                    if (stream_name is ALL or
                        d.get('name', 'primary') == stream_name)]
        except self.NoEventDescriptors:
            return []

    def descriptor_given_uid(self, desc_uid):
        return self.mds.descriptor_given_uid(desc_uid)

    def docs_given_header(self, header, stream_name=ALL, fields=None):
        """Get documents for given Header.

        Parameters
        ----------
        header : Header
            The headers to fetch the events for
        stream_name : string, optional
            Get events from only one "event stream" with this
            name. Default value is special sentinel class, `ALL`,
            which gets all streams together.
        fields : list, optional
            whitelist of field names of interest or regular expression;
            if None, all are returned
        Yields
        ------
        str : name
            The name of the document being yielded
        doc : Document
            The data payload

        """
        no_fields_filter = False
        if fields is None:
            no_fields_filter = True
            fields = []
        fields = set(fields)

        comp_re = _compile_re(fields)

        descs = self.descriptors_given_header(header, stream_name)

        start = header['start']
        stop = header['stop']

        yield 'start', header['start']
        ev_gens = []
        per_desc_discards = {}
        per_desc_extra_data = {}
        per_desc_extra_ts = {}
        for d in descs:
            (all_extra_dk, all_extra_data,
             all_extra_ts, discard_fields) = _extract_extra_data(
                start, stop, d, fields, comp_re, no_fields_filter)

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

            yield 'descriptor', d
            ev_gens.append(self.mds.get_events_generator(d))
        for ev in interlace_gens(*ev_gens):
            event_data = ev['data']  # cache for perf
            desc = ev['descriptor']
            event_timestamps = ev['timestamps']
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

            yield 'event', ev

        yield 'stop', header['stop']

    def table_given_header(self, header, stream_name,
                           fields=None, convert_times=True, timezone=None,
                           localize_times=True):
        """Make a table (pandas.DataFrame) from given header.

        Parameters
        ----------
        header : Header
            The header to fetch the table for
        fields : list, optional
            whitelist of field names of interest; if None, all are returned
        stream_name : string, optional Get data from a single "event
            stream." To obtain one comprehensive table with all
            streams, use `stream_name=ALL` (where `ALL` is a sentinel
            class defined in this module). The default name is
            'primary', but if no event stream with that name is found,
            the default reverts to `ALL` (for backward-compatibility).
        convert_times : bool, optional
            Whether to convert times from float (seconds since 1970) to
            numpy datetime64, using pandas. True by default, returns naive
            datetime64 objects in UTC
        timezone : str, optional
            e.g., 'US/Eastern'
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
        if timezone is None:
            timezone = self.mds.config['timezone']

        no_fields_filter = False
        if fields is None:
            no_fields_filter = True
            fields = []
        fields = set(fields)

        comp_re = _compile_re(fields)

        descs = self.descriptors_given_header(header, stream_name)

        start = header['start']
        stop = header.get('stop', {})
        descs = self.descriptors_given_header(header, stream_name)
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
                         .dt.tz_localize('UTC')  # first make tz aware
                         .dt.tz_convert(timezone)  # convert to 'local'
                         .dt.tz_localize(None)  # make naive again
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

            for field, v in all_extra_data.items():
                df[field] = v

            dfs.append(df)

        if dfs:
            return pd.concat(dfs)
        else:
            # edge case: no data
            return pd.DataFrame()

    def fill_event(self, ev, inplace=False, fields=None,
                   handler_registry=None, handler_overrides=None):
        """Fill by de-referencing

        fill : bool, optional
            Whether externally-stored data should be
            filled in. Defaults to False.
        fields : list, optional
            whitelist of field names of interest or regular expression;
            if None, all are returned
        handler_registry : dict, optional
            mapping asset specs (strings) to handlers (callable classes)
        handler_overrides : dict, optional
            mapping data keys (strings) to handlers (callable classes)
        """
        descriptor = self.mds.descriptor_given_uid(ev['descriptor'])
        external_map = _external_keys(descriptor)

        if fields is None:
            fields = set(k for k, v in external_map.items() if v is not None)

        if not inplace:
            ev = ev.copy()
            # reach in and cheat >:)
            dict.__setitem__(ev, 'data', ev['data'].copy())
            dict.__setitem__(ev, 'filled', ev['filled'].copy())

        data = ev['data']
        filled = ev['filled']
        # fast path with no by key name overrides
        if not handler_overrides:
            with self.fs.handler_context(handler_registry):
                for k in fields:
                    datum_uid = data[k]
                    data[k] = self.fs.retrieve(datum_uid)
                    filled[k] = datum_uid
        else:
            mock_registries = {dk: defaultdict(lambda: handler)
                               for dk, handler in handler_overrides.items()}

            for k in fields:
                with self.fs.handler_context(
                        mock_registries.get(k, handler_registry)):
                    datum_uid = data[k]
                    data[k] = self.fs.retrieve(datum_uid)
                    filled[k] = datum_uid
        return ev

    def fill_event_stream(self, ev_gen, d, inplace=False, fields=None,
                          handler_registry=None,
                          handler_overrides=None):

        external_map = _external_keys(d)

        if fields is None:
            fields = set(k for k, v in external_map.items() if v is not None)

        # fast path with no by key name overrides
        if not handler_overrides:
            with self.fs.handler_context(handler_registry):
                for ev in ev_gen:
                    if not inplace:
                        ev = ev.copy()
                        # reach in and cheat >:)
                        dict.__setitem__(ev, 'data', ev['data'].copy())
                        dict.__setitem__(ev, 'filled', ev['filled'].copy())

                    data = ev['data']
                    filled = ev['filled']
                    for k in fields:
                        datum_uid = data[k]
                        data[k] = self.fs.retrieve(datum_uid)
                        filled[k] = datum_uid
                    yield ev
        else:
            for ev in ev_gen:
                yield self.fill_event(ev, inplace, fields,
                                      handler_registry,
                                      handler_overrides)

    def fill_table(self, tab, descriptor, inplace=False,
                   handler_registry=None, handler_overrides=None):
        external_map = _external_keys(descriptor)
        if handler_overrides is None:
            handler_overrides = {}
        mock_registries = {data_key: defaultdict(lambda: handler)
                           for data_key, handler in
                           handler_overrides.items()}
        if not inplace:
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
                    values = [_fs.retrieve(value)
                              for value in tab[field]]
                tab[field] = values
        return tab


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


def _external_keys(descriptor, _cache=boltons.cacheutils.LRU(max_size=500)):
    """Which data keys are stored externally

    Parameters
    ----------
    descriptor : Doct
        The descriptor

    _cache : Mapping
        Cache to use.  Defaults to a boltons LRU closed over via
        mutable defaults.

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


def check_fields_exist(fields, headers):
    # uses _compile_re; not used here by used by databroker/broker.py
    comp_re = _compile_re(fields)
    all_fields = set()
    for header in headers:
        all_fields.update(header['start'])
        all_fields.update(header.get('stop', {}))
        for descriptor in header['descriptors']:
            all_fields.update(descriptor['data_keys'])
            objs_conf = descriptor.get('configuration', {})
            config_fields = [obj_conf['data']
                             for obj_conf in objs_conf.values()]
            all_fields.update(chain(*config_fields))
    missing = len(set(filter(comp_re.match, all_fields))) > 0
    if not missing:
        raise ValueError("The fields %r were not found." % fields)
