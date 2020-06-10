from collections.abc import Iterable
from collections import defaultdict
from datetime import datetime
import pandas
import re
import warnings
import time
import humanize
import jinja2
import os
import shutil
from types import SimpleNamespace
import tzlocal
import xarray
import event_model
import intake
import pymongo
# Toolz and CyToolz have identical APIs -- same test suite, docstrings.
try:
    from cytoolz.dicttoolz import merge
except ImportError:
    from toolz.dicttoolz import merge

from .utils import (ALL, format_time, get_fields, wrap_in_deprecated_doct,
                    ensure_path_exists, lookup_config,
                    transpose)


# The v2 API is expected to grow more options for filled than just True/False
# (e.g. 'delayed') so it expects a string instead of a boolean.
_FILL = {True: 'yes', False: 'no'}


def temp_config():
    raise NotImplementedError("Use temp() instead, which returns a v1.Broker.")


def temp():
    from .v2 import temp
    catalog = temp()
    return Broker(catalog)


class Registry:
    """
    An accessor that serves as a backward-compatible shim for Broker.reg
    """
    def __init__(self, catalog):
        self._catalog = catalog

    @property
    def handler_reg(self):
        return self._catalog.handler_registry

    @property
    def root_map(self):
        return self._catalog.root_map

    def register_handler(self, key, handler, overwrite=False):
        return self._catalog.register_handler(
            key, handler, overwrite=overwrite)

    def deregister_handler(self, key):
        return self._catalog.deregister_handler(key)

    def copy_files(self, resource, new_root,
                   verify=False, file_rename_hook=None,
                   run_start_uid=None):
        """
        Copy files associated with a resource to a new directory.

        The registered handler must have a `get_file_list` method and the
        process running this method must have read/write access to both the
        source and destination file systems.

        This method does *not* update the assets dataregistry_template.

        Internally the resource level directory information is stored
        as two parts: the root and the resource_path.  The 'root' is
        the non-semantic component (typically a mount point) and the
        'resource_path' is the 'semantic' part of the file path.  For
        example, it is common to collect data into paths that look like
        ``/mnt/DATA/2016/04/28``.  In this case we could split this as
        ``/mnt/DATA`` as the 'root' and ``2016/04/28`` as the resource_path.

        Parameters
        ----------
        resource : Document
            The resource to move the files of

        new_root : str
            The new 'root' to copy the files into

        verify : bool, optional (False)
            Verify that the move happened correctly.  This currently
            is not implemented and will raise if ``verify == True``.

        file_rename_hook : callable, optional
            If provided, must be a callable with signature ::

               def hook(file_counter, total_number, old_name, new_name):
                   pass

            This will be run in the inner loop of the file copy step and is
            run inside of an unconditional try/except block.

        See Also
        --------
        `RegistryMoving.shift_root`
        `RegistryMoving.change_root`
        """
        if verify:
            raise NotImplementedError('Verification is not implemented yet')

        def rename_hook_wrapper(hook):
            if hook is None:
                def noop(n, total, old_name, new_name):
                    return
                return noop

            def safe_hook(n, total, old_name, new_name):
                try:
                    hook(n, total, old_name, new_name)
                except Exception:
                    pass
            return safe_hook

        file_rename_hook = rename_hook_wrapper(file_rename_hook)

        run_start_uid = resource.get('run_start', run_start_uid)
        if run_start_uid is None:
            raise ValueError(
                "If the Resource document has no `run_start` key, the "
                "caller must provide run_start_uid.")
        file_list = self._catalog[run_start_uid].get_file_list(resource)

        # check that all files share the same root
        old_root = resource.get('root')
        if not old_root:
            warnings.warn("There is no 'root' in this resource which "
                          "is required to be able to change the root. "
                          "Please use `fs.shift_root` to move some of "
                          "the path from the 'resource_path' to the "
                          "'root'.  For now assuming '/' as root")
            old_root = os.path.sep

        for f in file_list:
            if not f.startswith(old_root):
                raise RuntimeError('something is very wrong, the files '
                                   'do not all share the same root, ABORT')

        # sort out where new files should go
        new_file_list = [os.path.join(new_root,
                                      os.path.relpath(f, old_root))
                         for f in file_list]
        N = len(new_file_list)
        # copy the files to the new location
        for n, (fin, fout) in enumerate(zip(file_list, new_file_list)):
            # copy files
            file_rename_hook(n, N, fin, fout)
            ensure_path_exists(os.path.dirname(fout))
            shutil.copy2(fin, fout)

        return zip(file_list, new_file_list)


class Broker:
    """
    This supports the original Broker API but implemented on intake.Catalog.
    """
    def __init__(self, catalog, *, serializer=None,
                 external_fetchers=None):
        self._catalog = catalog
        self.__serializer = serializer
        self.external_fetchers = external_fetchers or {}
        self.prepare_hook = wrap_in_deprecated_doct
        self.aliases = {}
        self.filters = {}
        self.v2._Broker__v1 = self
        self._reg = Registry(catalog)

    @property
    def _serializer(self):
        if self.__serializer is None:
            # The method _get_serializer is an optional method implememented on
            # some Broker subclasses to support the Broker.insert() method,
            # which is pending deprecation.
            if hasattr(self._catalog, '_get_serializer'):
                self.__serializer = self._catalog._get_serializer()
        return self.__serializer

    @property
    def reg(self):
        "Registry of externally-stored data"
        return self._reg

    @property
    def name(self):
        return self._catalog.name

    @property
    def v1(self):
        "A self-reference. This makes v1.Broker and v2.Broker symmetric."
        return self

    @property
    def v2(self):
        "Accessor to the version 2 API."
        return self._catalog

    @classmethod
    def from_config(cls, config, auto_register=True, name=None):
        return from_config(
            config=config, auto_register=auto_register, name=name)

    def get_config(self):
        """
        Return the v0 config dict this was created from, or None if N/A.
        """
        if hasattr(self, '_config'):
            return self._config

    @classmethod
    def named(cls, name, auto_register=True):
        """
        Create a new Broker instance using a configuration file with this name.

        Configuration file search path:

        * ``~/.config/databroker/{name}.yml``
        * ``{python}/../etc/databroker/{name}.yml``
        * ``/etc/databroker/{name}.yml``

        where ``{python}`` is the location of the current Python binary, as
        reported by ``sys.executable``. It will use the first match it finds.

        Special Case: The name ``'temp'`` creates a new, temporary
        configuration. Subsequent calls to ``Broker.named('temp')`` will
        create separate configurations. Any data saved using this temporary
        configuration will not be accessible once the ``Broker`` instance has
        been deleted.

        Parameters
        ----------
        name : string
        auto_register : boolean, optional
            By default, automatically register built-in asset handlers (classes
            that handle I/O for externally stored data). Set this to ``False``
            to do all registration manually.

        Returns
        -------
        db : Broker
        """
        if name == 'temp':
            return temp()
        else:
            try:
                config = lookup_config(name)
            except FileNotFoundError:
                # Continue on to the v2 way.
                pass
            else:
                db = cls.from_config(config, auto_register=auto_register, name=name)
                return db
        catalog = getattr(intake.cat, name)
        return Broker(catalog)

    @property
    def fs(self):
        warnings.warn("fs is deprecated, use `db.reg` instead", stacklevel=2)
        return self.reg

    def stream_names_given_header(self):
        return list(self._catalog)

    def fetch_external(self, start, stop):
        return {k: func(start, stop) for
                k, func in self.external_fetchers.items()}

    def _patch_state(self, catalog):
        "Copy references to v1 state."
        catalog.v1.aliases = self.aliases
        catalog.v1.filters = self.filters
        catalog.v1.prepare_hook = self.prepare_hook

    def __call__(self, text_search=None, **kwargs):
        data_key = kwargs.pop('data_key', None)
        tz = tzlocal.get_localzone().zone
        if self.filters:
            filters = self.filters.copy()
            format_time(filters, tz)  # mutates in place
            catalog = self._catalog.search(filters)
            self._patch_state(catalog)
        else:
            catalog = self._catalog
        if text_search:
            kwargs.update({'$text': {'$search': text_search}})
        format_time(kwargs, tz)  # mutates in place
        result_catalog = catalog.search(kwargs)
        self._patch_state(result_catalog)
        return Results(self, result_catalog,
                       data_key)

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
        datasource = self._catalog[key]
        return Header(datasource)

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
        if handler_registry is not None:
            raise NotImplementedError("The handler_registry must be set when "
                                      "the Broker is initialized, usually specified "
                                      "in a configuration file.")

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

            def merge_config_into_event(event):
                # Mutate event in place, adding in data and timestamps from the
                # descriptor's 'configuration' key.
                event_data = event['data']  # cache for perf
                desc = event['descriptor']
                event_timestamps = event['timestamps']
                event_data.update(per_desc_extra_data[desc])
                event_timestamps.update(per_desc_extra_ts[desc])
                discard_fields = per_desc_discards[desc]
                for field in discard_fields:
                    del event_data[field]
                    del event_timestamps[field]

            get_documents_router = _GetDocumentsRouter(self.prepare_hook,
                                                       merge_config_into_event,
                                                       stream_name=stream_name)
            for name, doc in self._catalog[uid].canonical(fill=_FILL[bool(fill)],
                                                          strict_order=True):
                yield from get_documents_router(name, doc)

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

        if handler_registry is not None:
            raise NotImplementedError("The handler_registry must be set when "
                                      "the Broker is initialized, usually specified "
                                      "in a configuration file.")

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

        if handler_registry is not None:
            raise NotImplementedError(
                "The handler_registry must be set when "
                "the Broker is initialized, usually specified "
                "in a configuration file.")

        headers = _ensure_list(headers)
        # TODO --- Use local time I guess.
        # if timezone is None:
        #     timezone = self.mds.config['timezone']

        no_fields_filter = False
        if fields is None:
            no_fields_filter = True
            fields = []
        fields = set(fields)

        comp_re = _compile_re(fields)

        dfs = []
        for header in headers:
            descs = header.descriptors
            start = header.start
            stop = header.stop
            descs = [desc for desc in descs if desc.get('name') == stream_name]
            for descriptor in descs:
                (all_extra_dk, all_extra_data,
                 all_extra_ts, discard_fields) = _extract_extra_data(
                    start, stop, descriptor, fields, comp_re, no_fields_filter)

                all_events = [
                    doc for name, doc in
                    self.get_documents(header, stream_name=stream_name, fill=fill)
                    if name == 'event' and
                    doc['descriptor'] == descriptor['uid']]
                seq_nums = [ev['seq_num'] for ev in all_events]
                times = [ev['time'] for ev in all_events]
                keys = list(descriptor['data_keys'])
                data = transpose(all_events, keys, 'data')
                # timestamps = transpose(all_events, keys, 'timestamps')

                df = pandas.DataFrame(index=seq_nums)
                # if converting to datetime64 (in utc or 'local' tz)
                if convert_times or localize_times:
                    times = pandas.to_datetime(times, unit='s')
                # make sure this is a series
                times = pandas.Series(times, index=seq_nums)

                # if localizing to 'local' time
                if localize_times:
                    times = (times
                             .dt.tz_localize('UTC')  # first make tz aware
                             # .dt.tz_convert(timezone)  # convert to 'local'
                             .dt.tz_localize(None)  # make naive again
                             )

                df['time'] = times
                for field, values in data.items():
                    if field in discard_fields:
                        continue
                    df[field] = values
                if list(df.columns) == ['time']:
                    # no content
                    continue

                for field, v in all_extra_data.items():
                    df[field] = v

                dfs.append(df)

        if dfs:
            result = pandas.concat(dfs)
        else:
            # edge case: no data
            result = pandas.DataFrame()
        result.index.name = 'seq_num'
        return result

    def get_images(self, headers, name,
                   stream_name='primary',
                   handler_registry=None,):
        """
        This method is deprecated. Use Broker.get_documents instead.

        Load image data from one or more runs into a lazy array-like object.

        Parameters
        ----------
        headers : Header or list of Headers
        name : string
            field name (data key) of a detector
        handler_registry : dict, optional
            mapping spec names (strings) to handlers (callable classes)

        Examples
        --------
        >>> header = db[-1]
        >>> images = Images(header, 'my_detector_lightfield')
        >>> for image in images:
                # do something
        """
        # Defer this import so that pims is an optional dependency.
        from ._legacy_images import Images
        headers = _ensure_list(headers)
        datasets = [header.xarray_dask(stream_name=stream_name)
                    for header in headers]
        if handler_registry is not None:
            raise NotImplementedError(
                "The handler_registry parameter is no longer supported "
                "and must be None.")
        dataset = xarray.merge(datasets)
        data_array = dataset[name]
        return Images(data_array=data_array)

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

    def export(self, headers, db, new_root=None, copy_kwargs=None):
        """
        Serialize a list of runs.

        If a new_root is passed files associated with the run will be moved to
        this new location, and the corresponding resource document will be
        updated with the new_root.

        Parameters
        ----------
        headers : databroker.header
            one or more run headers that are going to be exported
        db : databroker.Broker
            an instance of databroker.Broker class that will be the target to
            export info
        new_root : str
            optional. root directory of files that are going to
            be exported
        copy_kwargs : dict or None
            passed through to the ``copy_files`` method on Registry;
            None by default

        Returns
        ------
        file_pairs : list
            list of (old_file_path, new_file_path) pairs generated by
            ``copy_files`` method on Registry.
        """
        if copy_kwargs is None:
            copy_kwargs = {}

        if isinstance(headers, Header):
            headers = [headers]

        file_pairs = []

        for header in headers:
            for name, doc in self._catalog[header.start['uid']].canonical(fill='no'):
                if name == 'event_page':
                    for event in event_model.unpack_event_page(doc):
                        db.insert('event', event)
                elif name == 'resource' and new_root:
                    copy_kwargs.setdefault('run_start_uid', header.start['uid'])
                    file_pairs.extend(self.reg.copy_files(doc, new_root, **copy_kwargs))
                    new_resource = doc.to_dict()
                    new_resource['root'] = new_root
                    db.insert(name, new_resource)
                else:
                    db.insert(name, doc)
        return file_pairs

    def export_size(self, headers):
        """
        Get the size of files associated with a list of headers.

        Parameters
        ----------
        headers : :class:databroker.Header:
            one or more headers that are going to be exported

        Returns
        -------
        total_size : float
            total size of all the files associated with the ``headers`` in Gb
        """
        headers = _ensure_list(headers)
        total_size = 0
        for header in headers:
            run = self._catalog[header.start['uid']]
            for name, doc in self._catalog[header.start['uid']].canonical(fill='no'):
                if name == 'resource':
                    for filepath in run.get_file_list(doc):
                        total_size += os.path.getsize(filepath)

        return total_size * 1e-9

    def insert(self, name, doc):
        if self._serializer is None:
            raise RuntimeError("No Serializer was configured for this.")
        warnings.warn(
            "The method Broker.insert may be removed in a future release of "
            "databroker.", PendingDeprecationWarning)
        self._serializer(name, doc)
        # Make a reasonable effort to keep the Catalog in sync with new data.
        if name == 'stop':
            self._catalog.force_reload()

    def fill_event(*args, **kwargs):
        raise NotImplementedError("This method is no longer supported. If you "
                                  "need this please contact the developers by "
                                  "opening an issue here: "
                                  "https://github.com/bluesky/databroker/issues/new ")

    def fill_events(*args, **kwargs):
        raise NotImplementedError("This method is no longer supported. If you "
                                  "need this please contact the developers by "
                                  "opening an issue here: "
                                  "https://github.com/bluesky/databroker/issues/new ")


class Header:
    """
    This supports the original Header API but implemented on intake's Entry.
    """
    def __init__(self, datasource):
        self.__data_source = datasource
        self.db = datasource.catalog_object.v1
        self.ext = None  # TODO
        md = datasource.describe()['metadata']
        self._start = md['start']
        self._stop = md['stop']
        self.ext = SimpleNamespace(
            **self.db.fetch_external(self.start, self.stop))

    @property
    def _data_source(self):
        return self.__data_source

    @property
    def start(self):
        return self.db.prepare_hook('start', self._start)

    @property
    def uid(self):
        return self._start['uid']

    @property
    def stop(self):
        if self._stop is None:
            self._stop = self.__data_source.describe()['metadata']['stop'] or {}
        return self.db.prepare_hook('stop', self._stop)

    def __eq__(self, other):
        return self.start == other.start

    @property
    def descriptors(self):
        descriptors = self._data_source._descriptors
        return sorted([self.db.prepare_hook('descriptor', doc) for doc in descriptors],
                      key=lambda d: d['time'], reverse=True)

    @property
    def stream_names(self):
        return list(self.__data_source)

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

    def xarray(self, stream_name='primary'):
        return self._data_source[stream_name].read()

    def xarray_dask(self, stream_name='primary'):
        return self._data_source[stream_name].to_dask()

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

    def devices(self, stream_name=ALL):
        """
        Return the names of the devices in this run.

        Parameters
        ----------
        stream_name : string or ``ALL``, optional
            Filter results by stream name (e.g., 'primary', 'baseline'). The
            default, ``ALL``, combines results from all streams.

        Returns
        -------
        devices : set

        Examples
        --------
        Load the most recent run and list its devices.

        >>> h = db[-1]
        >>> h.devices()
        {'eiger'}

        See Also
        --------
        :meth:`Header.fields`
        """
        result = set()
        for d in self.descriptors:
            if stream_name is ALL or stream_name == d.get('name', 'primary'):
                result.update(d['object_keys'])
        return result

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
    def __init__(self, broker, catalog, data_key):
        self._broker = broker
        self._catalog = catalog
        self._data_key = data_key

    def __iter__(self):
        # TODO Catalog.walk() fails. We should probably support Catalog.items().
        for uid, entry in self._catalog._entries.items():
            header = Header(entry())
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


class InvalidConfig(Exception):
    """Raised when the configuration file is invalid."""
    ...


def from_config(config, auto_register=True, name=None):
    """
    Build (some version of) a Broker instance from a v0 configuration dict.

    This method accepts v1 config files.

    This can return a ``v0.Broker``, ``v1.Broker``, or ``v2.Broker`` depending
    on the contents of ``config``.

    If config contains the key 'api_version', it should be set to a value 0, 1,
    0, or 2. That setting will be respected until there is an error, in which
    case a warning will be issued and we will fall back to v0. If no
    'api_version' is explicitly set by the configuration file, version 1 will
    be used.
    """
    forced_version = config.get('api_version')
    if forced_version == 0:
        from . import v0
        return v0.Broker.from_config(config, auto_register, name)
    try:
        catalog = _from_v0_config(config, auto_register, name)
    except InvalidConfig:
        raise
    except Exception as exc:
        warnings.warn(
            f"Failed to load config. Falling back to v0."
            f"Exception was: {exc}")
        from . import v0
        return v0.Broker.from_config(config, auto_register, name)
    if forced_version == 2:
        return catalog
    elif forced_version is None or forced_version == 1:
        broker = Broker(catalog)
        broker._config = config  # HACK to support Broker.get_config()
        return broker
    else:
        raise ValueError(f"Cannot handle api_version {forced_version}")


def _from_v0_config(config, auto_register, name):
    mds_module = config['metadatastore']['module']
    if mds_module != 'databroker.headersource.mongo':
        raise NotImplementedError(
            f"Unable to handle metadatastore.module {mds_module!r}")
    mds_class = config['metadatastore']['class']
    if mds_class not in ('MDS', 'MDSRO'):
        raise NotImplementedError(
            f"Unable to handle metadatastore.class {mds_class!r}")

    assets_module = config['assets']['module']
    if assets_module != 'databroker.assets.mongo':
        raise NotImplementedError(
            f"Unable to handle assets.module {assets_module!r}")
    assets_class = config['assets']['class']
    if assets_class not in ('Registry', 'RegistryRO'):
        raise NotImplementedError(
            f"Unable to handle assets.class {assets_class!r}")

    # Get the mongo databases.
    metadatastore_db = _get_mongo_database(config['metadatastore']['config'])
    asset_registry_db = _get_mongo_database(config['assets']['config'])

    from ._drivers.mongo_normalized import BlueskyMongoCatalog
    from .core import discover_handlers

    # Update the handler registry.
    handler_registry = {}
    if auto_register:
        handler_registry.update(discover_handlers())
    # In v0, config-specified handlers are *added* to any default ones.
    for spec, contents in config.get('handlers', {}).items():
        dotted_object = '.'.join((contents['module'], contents['class']))
        handler_registry[spec] = dotted_object

    root_map = config.get('root_map')
    transforms = config.get('transforms')

    return BlueskyMongoCatalog(metadatastore_db, asset_registry_db,
                               handler_registry=handler_registry,
                               root_map=root_map,
                               name=name,
                               transforms=transforms)


_mongo_clients = {}  # cache of pymongo.MongoClient instances


def _get_mongo_database(config):
    """
    Return a MongoClient.database. Use a cache in order to reuse the
    MongoClient.
    """
    # Check that config contains either uri, or host/port, but not both.
    if {'uri', 'host'} <= set(config) or {'uri', 'port'} <= set(config):
        raise InvalidConfig(
            "The config file must define either uri, or host/port, but not both.")

    uri = config.get('uri')
    database = config['database']

    # If this statement is True then uri does not exist in the config.
    # If the config has username and password, turn it into a uri.
    # This is only here for backward compatibility.
    if {'mongo_user', 'mongo_pwd', 'host', 'port'} <= set(config):
        uri = (f"mongodb://{config['mongo_user']}:{config['mongo_pwd']}@"
               "f{config['host']}:{config['port']}/")

    if uri:
        try:
            client = _mongo_clients[uri]
        except KeyError:
            client = pymongo.MongoClient(uri)
            _mongo_clients[uri] = client
    else:
        host = config.get('host')
        port = config.get('port')
        try:
            client = _mongo_clients[(host, port)]
        except KeyError:
            client = pymongo.MongoClient(host, port)
            _mongo_clients[(host, port)] = client

    return client[database]


class _GetDocumentsRouter:
    """
    This is used by Broker.get_documents.

    It employs a pattern similar to event_model.DocumentRouter, but the methods
    are generators instead of functions.
    """
    def __init__(self, prepare_hook, merge_config_into_event, stream_name):
        self.prepare_hook = prepare_hook
        self.merge_config_into_event = merge_config_into_event
        self.stream_name = stream_name
        self._descriptors = set()

    def __call__(self, name, doc):
        # Special case when there is no Run Stop doc.
        # In v0, we returned an empty dict here. We now think better of it.
        if name == 'stop' and doc is None:
            doc = {}
        for new_name, new_doc in getattr(self, name)(doc):
            yield new_name, self.prepare_hook(new_name, new_doc)

    def descriptor(self, doc):
        "Cache descriptor uid and pass it through if it is stream of interest."
        if self.stream_name is ALL or doc.get('name', 'primary') == self.stream_name:
            self._descriptors.add(doc['uid'])
            yield 'descriptor', doc

    def event_page(self, doc):
        "Unpack into events and pass them to event method for more processing."
        if doc['descriptor'] in self._descriptors:
            for event in event_model.unpack_event_page(doc):
                yield from self.event(event)

    def event(self, doc):
        "Apply merge_config_into_event."
        if doc['descriptor'] in self._descriptors:
            # Mutate event in place, merging in content from other documents
            # and discarding fields excluded by the user.
            self.merge_config_into_event(doc)
            # If the mutation above leaves event['data'] empty, omit it.
            if doc['data']:
                yield 'event', doc

    def datum_page(self, doc):
        "Unpack into datum."
        for datum in event_model.unpack_datum_page(doc):
            yield 'datum', datum

    def datum(self, doc):
        yield 'datum', doc

    def start(self, doc):
        yield 'start', doc

    def stop(self, doc):
        yield 'stop', doc

    def resource(self, doc):
        yield 'resource', doc
