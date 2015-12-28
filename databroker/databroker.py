from __future__ import print_function
import warnings
import six  # noqa
from collections import deque
import pandas as pd
import tzlocal
from metadataclient.commands import (find_last, find_run_starts,
                                    find_descriptors,
                                    get_events_generator, get_events_table)
import doct as doc
import metadatastore.commands as mc
import filestore.api as fs
import logging
import numbers


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


def get_events(headers, fields=None, fill=True):
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

    Yields
    ------
    event : Event
        The event, optionally with non-scalar data filled in
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

    for header in headers:
        descriptors = find_descriptors(header['start']['uid'])
        for descriptor in descriptors:
            all_fields = set(descriptor['data_keys'])
            if fields:
                discard_fields = all_fields - fields
            else:
                discard_fields = []
            if discard_fields == all_fields:
                continue
            for event in get_events_generator(descriptor):
                for field in discard_fields:
                    del event.data[field]
                    del event.timestamps[field]
                if fill:
                    fill_event(event)
                yield event


def get_table(headers, fields=None, fill=True, convert_times=True):
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

    if fields is None:
        fields = []
    fields = set(fields)

    dfs = []
    for header in headers:
        descriptors = find_descriptors(header['start']['uid'])
        for descriptor in descriptors:
            all_fields = set(descriptor['data_keys'])
            if fields:
                discard_fields = all_fields - fields
            else:
                discard_fields = []
            if discard_fields == all_fields:
                continue
            is_external = _inspect_descriptor(descriptor)

            payload = get_events_table(descriptor)
            descriptor, data, seq_nums, times, uids, timestamps = payload
            df = pd.DataFrame(index=seq_nums)
            if convert_times:
                times = pd.to_datetime(
                    pd.Series(times, index=seq_nums),
                    unit='s', utc=True).dt.tz_localize(TZ)
            df['time'] = times
            for field, values in six.iteritems(data):
                if field in discard_fields:
                    logger.debug('Discarding field %s', field)
                    continue
                if is_external[field] and fill:
                    logger.debug('filling data for %s', field)
                    # TODO someday we will have bulk retrieve in FS
                    values = [fs.retrieve(value) for value in values]
                df[field] = values
            dfs.append(df)
    if dfs:
        return pd.concat(dfs)
    else:
        # edge case: no data
        return pd.DataFrame()
