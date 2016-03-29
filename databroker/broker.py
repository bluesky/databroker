from __future__ import print_function
import warnings
import six  # noqa
from collections import deque, defaultdict

import tzlocal
import logging
import numbers
from .core import (Header, _inspect_descriptor,
                   get_events as _get_events,
                   get_table as _get_table,
                   restream as _restream,
                   process as _process, Images)


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
# py2: six.string_types = (basestring,)
# py3: six.string_types = (str,)
# so we need to just grab the only element out of this
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
    def __init__(self, mds, fs):
        self.mds = mds
        self.fs = fs

    def __getitem__(self, key):
        """DWIM slicing

        Some more docs go here
        """
        return search(key, self.mds)

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
        run_start = self.mds.find_run_starts(**kwargs)
        if data_key is not None:
            node_name = 'data_keys.{0}'.format(data_key)

            query = {node_name: {'$exists': True}}
            descriptors = []
            for rs in run_start:
                descriptor = self.mds.find_descriptors(run_start=rs, **query)
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
            result.append(Header.from_run_start(self.mds, rs))
        return result

    def find_headers(self, **kwargs):
        "This function is deprecated."
        warnings.warn("Use .__call__() instead of " ".find_headers()")
        return self(**kwargs)

    def fetch_events(self, headers, fill=True):
        "This function is deprecated."
        warnings.warn("Use .get_events() instead.")
        return self.get_events(headers, None, fill)

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
        if handler_overrides is None:
            handler_overrides = {}
        is_external = _inspect_descriptor(event.descriptor)
        mock_registries = {data_key: defaultdict(lambda: handler)
                        for data_key, handler in handler_overrides.items()}
        for data_key, value in six.iteritems(event.data):
            if is_external.get(data_key, False):
                if data_key not in handler_overrides:
                    with self.fs.handler_context(handler_registry) as fs:
                        event.data[data_key] = fs.get_datum(value)
                else:
                    mock_registry = mock_registries[data_key]
                    with self.fs.handler_context(mock_registry) as fs:
                        event.data[data_key] = fs.get_datum(value)

    def get_events(self, headers, fields=None, fill=True,
                   handler_registry=None, handler_overrides=None):
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
        res = _get_events(mds=self.mds, fs=self.fs, headers=headers,
                         fields=fields, fill=fill,
                         handler_registry=handler_registry,
                         handler_overrides=handler_overrides)
        for event in res:
            yield event


    def get_table(self, headers, fields=None, fill=True, convert_times=True,
                  timezone=None, handler_registry=None,
                  handler_overrides=None):
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
                         fields=fields, fill=fill, convert_times=convert_times,
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


    def restream(self, headers, fields=None, fill=True):
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
        res = _restream(self.mds, self.fs, headers, fields=None, fill=True)
        for name_doc_pair in res:
            yield name_doc_pair

    stream = restream  # compat

    def process(self, headers, func, fields=None, fill=True):
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
        _process(mds=self.mds, fs=self.fs, headers=headers, func=func,
                 fields=fields, fill=fill)
