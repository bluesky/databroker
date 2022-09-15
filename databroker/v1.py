from collections import defaultdict
from datetime import datetime
import pandas
import re
import warnings
import time
import humanize
import jinja2
import os
from types import SimpleNamespace

import xarray
import event_model

# Toolz and CyToolz have identical APIs -- same test suite, docstrings.
try:
    from cytoolz.dicttoolz import merge
except ImportError:
    from toolz.dicttoolz import merge

from tiled.client import from_profile
from tiled.client.utils import ClientError
from tiled.queries import FullText, Key

from .queries import TimeRange
from .utils import ALL, get_fields, wrap_in_deprecated_doct


# The v2 API is expected to grow more options for filled than just True/False
# (e.g. 'delayed') so it expects a string instead of a boolean.
_FILL = {True: "yes", False: "no"}


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
        warnings.warn(
            "In databroker 2.x, there are separate notions of 'server' and 'client', "
            "and root_map is not visible to the client. Likely these "
            "details are handled for you on the server side, so you should not worry "
            "about this message unless you encounter trouble loading large array data."
        )

    @property
    def root_map(self):
        warnings.warn(
            "In databroker 2.x, there are separate notions of 'server' and 'client', "
            "and root_map is not visible to the client. Likely these "
            "details are handled for you on the server side, so you should not worry "
            "about this message unless you encounter trouble loading large array data."
        )

    def register_handler(self, *args, **kwargs):
        warnings.warn(
            "In databroker 2.x, there are separate notions of 'server' and 'client', "
            "and register_handler(...) has no effect on the client. Likely this "
            "is being done for you on the server side, so you should not worry "
            "about this message unless you encounter trouble loading large array data."
        )

    def deregister_handler(self, key):
        warnings.warn(
            "In databroker 2.x, there are separate notions of 'server' and 'client', "
            "and deregister_handler(...) has no effect on the client. Likely this "
            "is being done for you on the server side, so you should not worry "
            "about this message unless you encounter trouble loading large array data."
        )

    def copy_files(
        self,
        resource,
        new_root,
        verify=False,
        file_rename_hook=None,
        run_start_uid=None,
    ):
        raise NotImplementedError(
            "The copy_files functionality is not supported via a client."
        )


def _no_aliases():
    raise NotImplementedError("Aliases have been removed. Use search instead.")


def _no_filters():
    raise NotImplementedError(
        """Filters have been removed. Chain searches instead like


>>> db.v2.search(...).search(...)""")


class Broker:
    """
    This supports the original Broker API but implemented on intake.Catalog.
    """

    def __init__(self, catalog):
        self._catalog = catalog
        self.prepare_hook = wrap_in_deprecated_doct
        self.v2._Broker__v1 = self
        self._reg = Registry(catalog)

        # When the user asks for a Serializer, give a RunRouter
        # that will generate a fresh Serializer instance for each
        # run.

        def factory(name, doc):
            return [self._catalog.get_serializer()], []

        self._run_router = event_model.RunRouter([factory])

    @property
    def aliases(self):
        _no_aliases()

    @aliases.setter
    def aliases(self, value):
        _no_aliases()

    @property
    def filters(self):
        _no_filters()

    @filters.setter
    def filters(self):
        _no_filters()

    def add_filter(self, *args, **kwargs):
        _no_filters()

    def clear_filters(self, *args, **kwargs):
        _no_filters()

    @property
    def _serializer(self):
        return self._run_router

    @property
    def reg(self):
        "Registry of externally-stored data"
        return self._reg

    @property
    def name(self):
        return self._catalog.metadata.get("name")

    @property
    def v1(self):
        "A self-reference. This makes v1.Broker and v2.Broker symmetric."
        return self

    @property
    def v2(self):
        "Accessor to the version 2 API."
        return self._catalog

    @classmethod
    def from_config(cls, config, auto_register=None, name=None):
        raise NotImplementedError(
            """Old-style databroker configuration is not supported.
"To construct from tiled profile, use:

    >>> from tiled.client import from_config
    >>> Broker(from_config({...}))
""")

    def get_config(self):
        raise NotImplementedError("No longer supported")

    @classmethod
    def named(cls, name, auto_register=None, try_raw=True):
        """
        Create a new Broker instance using the Tiled profile of this name.

        See https://blueskyproject.io/tiled/how-to/profiles.html

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
        try_raw: boolean, optional
            This is a backward-compatibilty shim. Raw data has been moved from
            "xyz" to "xyz/raw" in many deployments. If true, check to see if an item
            named "raw" is contained in this node and, if so, uses that.

        Returns
        -------
        db : Broker
        """
        if auto_register is not None:
            warnings.warn(
                "The parameter auto_register is now ignored. "
                "Handlers are now a concern of the service and not configurable "
                "from the client."
            )
        if name == "temp":
            raise NotImplementedError("databroker 2.0.0 does not yet support 'temp' Broker")
        client = from_profile(name)
        if try_raw:
            try:
                client = client["raw"]
            except (ClientError, KeyError):
                pass
        return Broker(client)

    @property
    def fs(self):
        warnings.warn("fs is deprecated, use `db.reg` instead", stacklevel=2)
        return self.reg

    def stream_names_given_header(self):
        return list(self._catalog)

    def _patch_state(self, catalog):
        "Copy references to v1 state."
        catalog.v1.prepare_hook = self.prepare_hook

    def __call__(self, text_search=None, **kwargs):
        results_catalog = self._catalog
        since = kwargs.pop("since", None) or kwargs.pop("start_time", None)
        until = kwargs.pop("until", None) or kwargs.pop("stop_time", None)
        if (since is not None) or (until is not None):
            results_catalog = results_catalog.search(
                TimeRange(since=since, until=until)
            )
        if "data_key" in kwargs:
            raise NotImplementedError("Search by data key is no longer implemented.")
        for key, value in kwargs.items():
            results_catalog = results_catalog.search(Key(key) == value)
        if text_search:
            results_catalog = results_catalog.search(FullText(text_search))
        self._patch_state(results_catalog)
        return Results(results_catalog)

    def __getitem__(self, key):
        result = self._catalog[key]
        if isinstance(result, list):
            # self[a, b, c] -> List[BlueskyRun]
            return [Header(run, self) for run in result]
        else:
            # self[a] -> BlueskyRun
            return Header(result, self)

    get_fields = staticmethod(get_fields)

    def get_documents(
        self, headers, stream_name=ALL, fields=None, fill=False, handler_registry=None
    ):
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
            raise NotImplementedError(
                "The handler_registry must be set when "
                "the Broker is initialized, usually specified "
                "in a configuration file."
            )

        headers = _ensure_list(headers)

        no_fields_filter = False
        if fields is None:
            no_fields_filter = True
            fields = []
        fields = set(fields)

        comp_re = _compile_re(fields)

        for header in headers:
            uid = header.start["uid"]
            descs = header.descriptors

            per_desc_discards = {}
            per_desc_extra_data = {}
            per_desc_extra_ts = {}
            for d in descs:
                (
                    all_extra_dk,
                    all_extra_data,
                    all_extra_ts,
                    discard_fields,
                ) = _extract_extra_data(
                    header.start, header.stop, d, fields, comp_re, no_fields_filter
                )

                per_desc_discards[d["uid"]] = discard_fields
                per_desc_extra_data[d["uid"]] = all_extra_data
                per_desc_extra_ts[d["uid"]] = all_extra_ts

                d = d.copy()
                dict.__setitem__(d, "data_keys", d["data_keys"].copy())
                for k in discard_fields:
                    del d["data_keys"][k]
                d["data_keys"].update(all_extra_dk)

                if not len(d["data_keys"]) and not len(all_extra_data):
                    continue

            def merge_config_into_event(event):
                # Mutate event in place, adding in data and timestamps from the
                # descriptor's 'configuration' key.
                event_data = event["data"]  # cache for perf
                desc = event["descriptor"]
                event_timestamps = event["timestamps"]
                event_data.update(per_desc_extra_data[desc])
                event_timestamps.update(per_desc_extra_ts[desc])
                discard_fields = per_desc_discards[desc]
                for field in discard_fields:
                    del event_data[field]
                    del event_timestamps[field]

            get_documents_router = _GetDocumentsRouter(
                self.prepare_hook, merge_config_into_event, stream_name=stream_name
            )
            for name, doc in self._catalog[uid].documents(fill=fill):
                yield from get_documents_router(name, doc)

    def get_events(
        self,
        headers,
        stream_name="primary",
        fields=None,
        fill=False,
        handler_registry=None,
    ):
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
            raise NotImplementedError(
                "The handler_registry must be set when "
                "the Broker is initialized, usually specified "
                "in a configuration file."
            )

        for name, doc in self.get_documents(
            headers,
            fields=fields,
            stream_name=stream_name,
            fill=fill,
            handler_registry=handler_registry,
        ):
            if name == "event":
                yield doc

    def get_table(
        self,
        headers,
        stream_name="primary",
        fields=None,
        fill=False,
        handler_registry=None,
        convert_times=True,
        timezone=None,
        localize_times=True,
    ):
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
                "in a configuration file."
            )

        headers = _ensure_list(headers)
        fields = set(fields or [])
        dfs = []
        for header in headers:
            descriptors = [
                d for d in header.descriptors if d.get("name") == stream_name
            ]
            data_keys = descriptors[0]["data_keys"]
            if not fill:
                external_fields = {k for k, v in data_keys.items() if v.get("external")}
                requested_external = fields.intersection(external_fields)
                if requested_external:
                    raise ValueError(
                        f"The fields {requested_external} are externally stored data "
                        "and can only be requested with fill=True."
                    )
                applicable_fields = (fields or set(data_keys)) - external_fields
            else:
                applicable_fields = fields or set(data_keys)
            applicable_fields.add("time")
            run = self._catalog[header.start["uid"]]
            dataset = run[stream_name].read(variables=(applicable_fields or None))
            dataset.load()
            dict_of_arrays = {}
            for var_name in dataset:
                column = dataset[var_name][:].data
                if column.ndim > 1:
                    column = list(column)  # data must be 1-dimensional
                dict_of_arrays[var_name] = column
            df = pandas.DataFrame(dict_of_arrays)
            # if converting to datetime64 (in utc or 'local' tz)
            times = dataset["time"][:].data
            if convert_times or localize_times:
                times = pandas.to_datetime(times, unit="s")
            # make sure this is a series
            times = pandas.Series(times, index=df.index)

            # if localizing to 'local' time
            if localize_times:
                times = (
                    times.dt.tz_localize("UTC")  # first make tz aware
                    # .dt.tz_convert(timezone)  # convert to 'local'
                    .dt.tz_localize(None)  # make naive again
                )

            df["time"] = times
            dfs.append(df)
        if dfs:
            result = pandas.concat(dfs)
        else:
            # edge case: no data
            result = pandas.DataFrame()
        result.index.name = "seq_num"
        # seq_num starts at 1, not 0
        result.index = 1 + result.index
        return result

    def get_images(
        self,
        headers,
        name,
        stream_name="primary",
        handler_registry=None,
    ):
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
        datasets = [header.xarray_dask(stream_name=stream_name) for header in headers]
        if handler_registry is not None:
            raise NotImplementedError(
                "The handler_registry parameter is no longer supported "
                "and must be None."
            )
        dataset = xarray.merge(datasets)
        data_array = dataset[name]
        return Images(data_array=data_array)

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
            for name, doc in self._catalog[header.start["uid"]].documents(fill=False):
                if name == "event_page":
                    for event in event_model.unpack_event_page(doc):
                        db.insert("event", event)
                elif name == "resource" and new_root:
                    copy_kwargs.setdefault("run_start_uid", header.start["uid"])
                    file_pairs.extend(self.reg.copy_files(doc, new_root, **copy_kwargs))
                    new_resource = doc.to_dict()
                    new_resource["root"] = new_root
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
            run = self._catalog[header.start["uid"]]
            for name, doc in self._catalog[header.start["uid"]].documents(fill="no"):
                if name == "resource":
                    for filepath in run.get_file_list(doc):
                        total_size += os.path.getsize(filepath)

        return total_size * 1e-9

    def insert(self, name, doc):
        if self._serializer is None:
            raise RuntimeError("No Serializer was configured for this.")
        warnings.warn(
            "The method Broker.insert may be removed in a future release of "
            "databroker.",
            PendingDeprecationWarning,
        )
        self._serializer(name, doc)

    def fill_event(*args, **kwargs):
        raise NotImplementedError(
            "This method is no longer supported. If you "
            "need this please contact the developers by "
            "opening an issue here: "
            "https://github.com/bluesky/databroker/issues/new "
        )

    def fill_events(*args, **kwargs):
        raise NotImplementedError(
            "This method is no longer supported. If you "
            "need this please contact the developers by "
            "opening an issue here: "
            "https://github.com/bluesky/databroker/issues/new "
        )

    def stats(self):
        "Access MongoDB storage statistics for this database."
        return self.v2.stats()


class Header:
    """
    This supports the original Header API but implemplemented on new code..
    """

    def __init__(self, run, db):
        self._run = run
        self.db = db
        self.ext = None  # TODO
        self._start = self._run.metadata["start"]
        self._stop = self._run.metadata["stop"]
        self.ext = SimpleNamespace()  # not implemented

    @property
    def v2(self):
        return self._run

    @property
    def start(self):
        return self.db.prepare_hook("start", self._start)

    @property
    def uid(self):
        return self._start["uid"]

    @property
    def stop(self):
        if self._stop is None:
            self._stop = self._run.metadata["stop"] or {}
        return self.db.prepare_hook("stop", self._stop)

    def __eq__(self, other):
        if not isinstance(other, Header):
            return False
        return self.start == other.start

    @property
    def descriptors(self):
        descriptors = []
        for stream in self._run.values():
            descriptors.extend(stream.descriptors)
        return sorted(
            [self.db.prepare_hook("descriptor", doc) for doc in descriptors],
            key=lambda d: d["time"],
            reverse=True,
        )

    @property
    def stream_names(self):
        return list(self._run)

    # These methods mock part of the dict interface. It has been proposed that
    # we might remove them for 1.0.

    def __getitem__(self, k):
        if k in ("start", "descriptors", "stop", "ext"):
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
        for k in ("start", "descriptors", "stop", "ext"):
            yield k

    def __iter__(self):
        return self.keys()

    def xarray(self, stream_name="primary"):
        return self._run[stream_name].read()

    def xarray_dask(self, stream_name="primary"):
        return self._run[stream_name].to_dask()

    def table(
        self,
        stream_name="primary",
        fields=None,
        fill=False,
        timezone=None,
        convert_times=True,
        localize_times=True,
    ):
        """
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
        """
        return self.db.get_table(
            self,
            fields=fields,
            stream_name=stream_name,
            fill=fill,
            timezone=timezone,
            convert_times=convert_times,
            localize_times=localize_times,
        )

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
        gen = self.db.get_documents(
            self, fields=fields, stream_name=stream_name, fill=fill
        )
        for payload in gen:
            yield payload

    def data(self, field, stream_name="primary", fill=True):
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
        if not fill:
            raise ValueError("Only fill=True is now supported by the data(...) method.")
        for item in self._run[stream_name]["data"][field][:]:
            yield item

    def stream(self, *args, **kwargs):
        warnings.warn(
            "The 'stream' method been renamed to 'documents'. The old name "
            "will be removed in the future."
        )
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
            if stream_name is ALL or descriptor.get("name") == stream_name:
                fields.update(descriptor["data_keys"])
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
            if stream_name is ALL or stream_name == d.get("name", "primary"):
                result.update(d["object_keys"])
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
        for d in sorted(self.descriptors, key=lambda d: d["time"]):
            config = d["configuration"].get(device_name)
            if config:
                result[d.get("name")].append(config["data"])
        return dict(result)  # strip off defaultdict behavior

    def events(self, stream_name="primary", fields=None, fill=False):
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
        ev_gen = self.db.get_events(
            [self], stream_name=stream_name, fields=fields, fill=fill
        )
        for ev in ev_gen:
            yield ev

    def _repr_html_(self):
        env = jinja2.Environment()
        env.filters["human_time"] = _pretty_print_time
        template = env.from_string(_HTML_TEMPLATE)
        return template.render(document=self)


class Results:
    """
    Iterable object encapsulating a results set of Headers

    Parameters
    ----------
    catalog : Catalog
        search results
    """

    def __init__(self, catalog):
        self._catalog = catalog
        self._broker = Broker(catalog)
        self._broker.v1.prepare_hook = catalog.v1.prepare_hook

    def __iter__(self):
        for uid, run in self._catalog.items():
            yield Header(run, self._broker)


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
        fields = [".*"]
    f = ["(?:" + regex + r")\Z" for regex in fields]
    comp_re = re.compile("|".join(f))
    return comp_re


def _extract_extra_data(start, stop, d, fields, comp_re, no_fields_filter):
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
        fields = set(filter(comp_re.match, source_data)) - selected_fields
        data = {k: source_data[k] for k in fields}
        timestamps = {k: source_ts[k] for k in fields}

        return {}, data, timestamps

    if fields:
        event_fields = set(d["data_keys"])
        selected_fields = set(filter(comp_re.match, event_fields))
        discard_fields = event_fields - selected_fields
    else:
        discard_fields = set()
        selected_fields = set(d["data_keys"])

    objs_config = d.get("configuration", {}).values()
    config_data = merge(obj_conf["data"] for obj_conf in objs_config)
    config_ts = merge(obj_conf["timestamps"] for obj_conf in objs_config)
    all_extra_data = {}
    all_extra_ts = {}
    all_extra_dk = {}
    if not no_fields_filter:
        for dt, ts in [
            (config_data, config_ts),
            (start, defaultdict(lambda: start["time"])),
            (stop, defaultdict(lambda: stop["time"])),
        ]:
            # Look in the descriptor, then start, then stop.
            l_dk, l_data, l_ts = _project_header_data(dt, ts, selected_fields, comp_re)
            all_extra_data.update(l_data)
            all_extra_ts.update(l_ts)
            selected_fields.update(l_data)
            all_extra_dk.update(l_dk)

    return (all_extra_dk, all_extra_data, all_extra_ts, discard_fields)


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
    return "{ago} ({date})".format(ago=ago, date=dt)


class InvalidConfig(Exception):
    """Raised when the configuration file is invalid."""

    ...


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
        if name == "stop" and doc is None:
            doc = {}
        for new_name, new_doc in getattr(self, name)(doc):
            yield new_name, self.prepare_hook(new_name, new_doc)

    def descriptor(self, doc):
        "Cache descriptor uid and pass it through if it is stream of interest."
        if self.stream_name is ALL or doc.get("name", "primary") == self.stream_name:
            self._descriptors.add(doc["uid"])
            yield "descriptor", doc

    def event_page(self, doc):
        "Unpack into events and pass them to event method for more processing."
        if doc["descriptor"] in self._descriptors:
            for event in event_model.unpack_event_page(doc):
                yield from self.event(event)

    def event(self, doc):
        "Apply merge_config_into_event."
        if doc["descriptor"] in self._descriptors:
            # Mutate event in place, merging in content from other documents
            # and discarding fields excluded by the user.
            self.merge_config_into_event(doc)
            # If the mutation above leaves event['data'] empty, omit it.
            if doc["data"]:
                yield "event", doc

    def datum_page(self, doc):
        "Unpack into datum."
        for datum in event_model.unpack_datum_page(doc):
            yield "datum", datum

    def datum(self, doc):
        yield "datum", doc

    def start(self, doc):
        yield "start", doc

    def stop(self, doc):
        yield "stop", doc

    def resource(self, doc):
        yield "resource", doc


_mongo_clients = {}  # cache of pymongo.MongoClient instances


def _get_mongo_database(config):
    """
    Return a MongoClient.database. Use a cache in order to reuse the
    MongoClient.
    """
    import pymongo

    # Check that config contains either uri, or host/port, but not both.
    if {"uri", "host"} <= set(config) or {"uri", "port"} <= set(config):
        raise InvalidConfig(
            "The config file must define either uri, or host/port, but not both."
        )

    uri = config.get("uri")
    database = config["database"]

    # If this statement is True then uri does not exist in the config.
    # If the config has username and password, turn it into a uri.
    # This is only here for backward compatibility.
    if {"mongo_user", "mongo_pwd", "host", "port"} <= set(config):
        uri = (
            f"mongodb://{config['mongo_user']}:{config['mongo_pwd']}@"
            f"{config['host']}:{config['port']}/"
        )

    if uri:
        if "authsource" in config:
            uri += f'?authsource={config["authsource"]}'

        try:
            client = _mongo_clients[uri]
        except KeyError:
            client = pymongo.MongoClient(uri)
            _mongo_clients[uri] = client
    else:
        host = config.get("host")
        port = config.get("port")
        try:
            client = _mongo_clients[(host, port)]
        except KeyError:
            client = pymongo.MongoClient(host, port)
            _mongo_clients[(host, port)] = client

    return client[database]
