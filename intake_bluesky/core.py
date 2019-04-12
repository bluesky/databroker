import collections
import event_model
from datetime import datetime
import dask
import dask.bag
import importlib
import itertools
import intake.catalog.base
import intake.container.base
import intake_xarray.base
from intake.compat import unpack_kwargs
import msgpack
import requests
from requests.compat import urljoin
import numpy
import warnings
import xarray


def documents_to_xarray(*, start_doc, stop_doc, descriptor_docs, event_docs,
                        filler, get_resource, get_datum, get_datum_cursor,
                        include=None, exclude=None):
    """
    Represent the data in one Event stream as an xarray.

    Parameters
    ----------
    start_doc: dict
        RunStart Document
    stop_doc : dict
        RunStop Document
    descriptor_docs : list
        EventDescriptor Documents
    event_docs : list
        Event Documents
    filler : event_model.Filler
    get_resource : callable
        Expected signature ``get_resource(resource_uid) -> Resource``
    get_datum : callable
        Expected signature ``get_datum(datum_id) -> Datum``
    get_datum_cursor : callable
        Expected signature ``get_datum_cursor(resource_uid) -> generator``
        where ``generator`` yields Datum documents
    include : list, optional
        Fields ('data keys') to include. By default all are included. This
        parameter is mutually exclusive with ``exclude``.
    exclude : list, optional
        Fields ('data keys') to exclude. By default none are excluded. This
        parameter is mutually exclusive with ``include``.

    Returns
    -------
    dataset : xarray.Dataset
    """
    if include is None:
        include = []
    if exclude is None:
        exclude = []
    if include and exclude:
        raise ValueError(
            "The parameters `include` and `exclude` are mutually exclusive.")

    # Data keys must not change within one stream, so we can safely sample
    # just the first Event Descriptor.
    data_keys = descriptor_docs[0]['data_keys']
    if include:
        keys = list(set(data_keys) & set(include))
    elif exclude:
        keys = list(set(data_keys) - set(exclude))
    else:
        keys = list(data_keys)

    # Collect a Dataset for each descriptor. Merge at the end.
    datasets = []
    for descriptor in descriptor_docs:
        events = [doc for doc in event_docs
                  if doc['descriptor'] == descriptor['uid']]
        if any(data_keys[key].get('external') for key in keys):
            for event in events:
                try:
                    filler('event', event)
                except event_model.UnresolvableForeignKeyError as err:
                    datum_id = err.key
                    datum = get_datum(datum_id)
                    resource_uid = datum['resource']
                    resource = get_resource(resource_uid)
                    filler('resource', resource)
                    # Pre-fetch all datum for this resource.
                    for datum in get_datum_cursor(resource_uid):
                        filler('datum', datum)
                    # TODO -- When to clear the datum cache in filler?
                    filler('event', event)
        times = [ev['time'] for ev in events]
        seq_nums = [ev['seq_num'] for ev in events]
        uids = [ev['uid'] for ev in events]
        data_table = _transpose(events, keys, 'data')
        # external_keys = [k for k in data_keys if 'external' in data_keys[k]]

        # Collect a DataArray for each field in Event, each field in
        # configuration, and 'seq_num'. The Event 'time' will be the
        # default coordinate.
        data_arrays = {}

        # Make DataArrays for Event data.
        for key in keys:
            field_metadata = data_keys[key]
            # Verify the actual ndim by looking at the data.
            ndim = numpy.asarray(data_table[key][0]).ndim
            dims = None
            if 'dims' in field_metadata:
                # As of this writing no Devices report dimension names ('dims')
                # but they could in the future.
                reported_ndim = len(field_metadata['dims'])
                if reported_ndim == ndim:
                    dims = tuple(field_metadata['dims'])
                else:
                    # TODO Warn
                    ...
            if dims is None:
                # Construct the same default dimension names xarray would.
                dims = tuple(f'dim_{i}' for i in range(ndim))
            data_arrays[key] = xarray.DataArray(
                data=data_table[key],
                dims=('time',) + dims,
                coords={'time': times},
                name=key)

        # Make DataArrays for configuration data.
        for object_name, config in descriptor['configuration'].items():
            data_keys = config['data_keys']
            # For configuration, label the dimension specially to
            # avoid key collisions.
            scoped_data_keys = {key: f'{object_name}:{key}'
                                for key in data_keys}
            if include:
                keys = {k: v for k, v in scoped_data_keys.items()
                        if v in include}
            elif exclude:
                keys = {k: v for k, v in scoped_data_keys.items()
                        if v not in include}
            else:
                keys = scoped_data_keys
            for key, scoped_key in keys.items():
                field_metadata = data_keys[key]
                # Verify the actual ndim by looking at the data.
                ndim = numpy.asarray(config['data'][key]).ndim
                dims = None
                if 'dims' in field_metadata:
                    # As of this writing no Devices report dimension names ('dims')
                    # but they could in the future.
                    reported_ndim = len(field_metadata['dims'])
                    if reported_ndim == ndim:
                        dims = tuple(field_metadata['dims'])
                    else:
                        # TODO Warn
                        ...
                if dims is None:
                    # Construct the same default dimension names xarray would.
                    dims = tuple(f'dim_{i}' for i in range(ndim))
                data_arrays[scoped_key] = xarray.DataArray(
                    # TODO Once we know we have one Event Descriptor
                    # per stream we can be more efficient about this.
                    data=numpy.tile(config['data'][key],
                                    (len(times),) + ndim * (1,)),
                    dims=('time',) + dims,
                    coords={'time': times},
                    name=key)

        # Finally, make DataArrays for 'seq_num' and 'uid'.
        data_arrays['seq_num'] = xarray.DataArray(
            data=seq_nums,
            dims=('time',),
            coords={'time': times},
            name='seq_num')
        data_arrays['uid'] = xarray.DataArray(
            data=uids,
            dims=('time',),
            coords={'time': times},
            name='uid')

        datasets.append(xarray.Dataset(data_vars=data_arrays))
    # Merge Datasets from all Event Descriptors into one representing the
    # whole stream. (In the future we may simplify to one Event Descriptor
    # per stream, but as of this writing we must account for the
    # possibility of multiple.)
    return xarray.merge(datasets)


class RemoteRunCatalog(intake.catalog.base.RemoteCatalog):
    """
    Catalog representing one Run.

    This is a client-side proxy to a RunCatalog stored on a remote server.

    Parameters
    ----------
    url: str
        Address of the server
    headers: dict
        HTTP headers to sue in calls
    name: str
        handle to reference this data
    parameters: dict
        To pass to the server when it instantiates the data source
    metadata: dict
        Additional info
    kwargs: ignored
    """
    name = 'bluesky-run-catalog'

    def __init__(self, url, http_args, name, parameters, metadata=None, **kwargs):
        super().__init__(url=url, http_args=http_args, name=name,
                         metadata=metadata)
        self.url = url
        self.name = name
        self.parameters = parameters
        self.http_args = http_args
        self._source_id = None
        self.metadata = metadata or {}
        self._get_source_id()
        self.bag = None

    def _get_source_id(self):
        if self._source_id is None:
            payload = dict(action='open', name=self.name,
                           parameters=self.parameters)
            req = requests.post(urljoin(self.url, '/v1/source'),
                                data=msgpack.packb(payload, use_bin_type=True),
                                **self.http_args)
            req.raise_for_status()
            response = msgpack.unpackb(req.content, **unpack_kwargs)
            self._parse_open_response(response)

    def _parse_open_response(self, response):
        self.npartitions = response['npartitions']
        self.metadata = response['metadata']
        self._schema = intake.source.base.Schema(
            datashape=None, dtype=None,
            shape=self.shape,
            npartitions=self.npartitions,
            metadata=self.metadata)
        self._source_id = response['source_id']

    def _load_metadata(self):
        if self.bag is None:
            self.parts = [dask.delayed(intake.container.base.get_partition)(
                self.url, self.http_args, self._source_id, self.container, i
            )
                          for i in range(self.npartitions)]
            self.bag = dask.bag.from_delayed(self.parts)
        return self._schema

    def _get_partition(self, i):
        self._load_metadata()
        return self.parts[i].compute()

    def read(self):
        raise NotImplementedError(
            "Reading the RunCatalog itself is not supported. Instead read one "
            "its entries, representing individual Event Streams.")

    def to_dask(self):
        raise NotImplementedError(
            "Reading the RunCatalog itself is not supported. Instead read one "
            "its entries, representing individual Event Streams.")

    def _close(self):
        self.bag = None

    def read_canonical(self):
        for i in range(self.npartitions):
            for name, doc in self._get_partition(i):
                yield name, doc

    def __repr__(self):
        self._load()
        try:
            start = self.metadata['start']
            stop = self.metadata['stop']
            out = (f"Run Catalog\n"
                   f"  uid={start['uid']!r}\n"
                   f"  exit_status={stop.get('exit_status')!r}\n"
                   f"  {_ft(start['time'])} -- {_ft(stop.get('time', '?'))}\n"
                   f"  Streams:\n")
            for stream_name in self:
                out += f"    * {stream_name}\n"
        except Exception as exc:
            out = f"<Intake catalog: Run *REPR_RENDERING_FAILURE* {exc!r}>"
        return out

    def search(self):
        raise NotImplementedError("Cannot search within one run.")


class RunCatalog(intake.catalog.Catalog):
    """
    Catalog representing one Run.

    Parameters
    ----------
    get_run_start: callable
        Expected signature ``get_run_start() -> RunStart``
    get_run_stop : callable
        Expected signature ``get_run_stop() -> RunStop``
    get_event_descriptors : callable
        Expected signature ``get_event_descriptors() -> List[EventDescriptors]``
    get_event_cursor : callable
        Expected signature ``get_event_cursor(descriptor_uids) -> generator``
        where ``generator`` yields Event documents
    get_resource : callable
        Expected signature ``get_resource(resource_uid) -> Resource``
    get_datum : callable
        Expected signature ``get_datum(datum_id) -> Datum``
    get_datum_cursor : callable
        Expected signature ``get_datum_cursor(resource_uid) -> generator``
        where ``generator`` yields Datum documents
    filler : event_model.Filler
    **kwargs :
        Additional keyword arguments are passed through to the base class,
        Catalog.
    """
    container = 'bluesky-run-catalog'
    version = '0.0.1'
    partition_access = True
    PARTITION_SIZE = 100

    def __init__(self,
                 get_run_start,
                 get_run_stop,
                 get_event_descriptors,
                 get_event_cursor,
                 get_event_count,
                 get_resource,
                 get_datum,
                 get_datum_cursor,
                 filler,
                 **kwargs):
        # All **kwargs are passed up to base class. TODO: spell them out
        # explicitly.
        self.urlpath = ''  # TODO Not sure why I had to add this.

        self._get_run_start = get_run_start
        self._get_run_stop = get_run_stop
        self._get_event_descriptors = get_event_descriptors
        self._get_event_cursor = get_event_cursor
        self._get_event_count = get_event_count
        self._get_resource = get_resource
        self._get_datum = get_datum
        self._get_datum_cursor = get_datum_cursor
        self.filler = filler
        super().__init__(**kwargs)

    def __repr__(self):
        try:
            start = self._run_start_doc
            stop = self._run_stop_doc or {}
            out = (f"Run Catalog\n"
                   f"  uid={start['uid']!r}\n"
                   f"  exit_status={stop.get('exit_status')!r}\n"
                   f"  {_ft(start['time'])} -- {_ft(stop.get('time', '?'))}\n"
                   f"  Streams:\n")
            for stream_name in self:
                out += f"    * {stream_name}\n"
        except Exception as exc:
            out = f"<Intake catalog: Run *REPR_RENDERING_FAILURE* {exc!r}>"
        return out

    def _load(self):
        # Count the total number of documents in this run.
        self._run_stop_doc = self._get_run_stop()
        self._run_start_doc = self._get_run_start()
        self._descriptors = self._get_event_descriptors()
        self._offset = len(self._descriptors) + 1
        self.metadata.update({'start': self._run_start_doc})
        self.metadata.update({'stop': self._run_stop_doc})

        count = 1
        descriptor_uids = [doc['uid'] for doc in self._descriptors]
        count += len(descriptor_uids)
        count += self._get_event_count(
            descriptor_uids=[doc['uid'] for doc in self._descriptors])
        count += (self._run_stop_doc is not None)
        self.npartitions = int(numpy.ceil(count / self.PARTITION_SIZE))

        self._schema = intake.source.base.Schema(
            datashape=None,
            dtype=None,
            shape=(count,),
            npartitions=self.npartitions,
            metadata=self.metadata)

        # Make a BlueskyEventStream for each stream_name.
        for doc in self._descriptors:
            if 'name' not in doc:
                warnings.warn(
                    f"EventDescriptor {doc['uid']!r} has no 'name', likely "
                    f"because it was generated using an old version of "
                    f"bluesky. The name 'primary' will be used.")
        descriptors_by_name = collections.defaultdict(list)
        for doc in self._descriptors:
            descriptors_by_name[doc.get('name', 'primary')].append(doc)
        for stream_name, descriptors in descriptors_by_name.items():
            args = dict(
                get_run_start=self._get_run_start,
                stream_name=stream_name,
                get_run_stop=self._get_run_stop,
                get_event_descriptors=self._get_event_descriptors,
                get_event_cursor=self._get_event_cursor,
                get_event_count=self._get_event_count,
                get_resource=self._get_resource,
                get_datum=self._get_datum,
                get_datum_cursor=self._get_datum_cursor,
                filler=self.filler,
                metadata={'descriptors': descriptors})
            self._entries[stream_name] = intake.catalog.local.LocalCatalogEntry(
                name=stream_name,
                description={},  # TODO
                driver='intake_bluesky.core.BlueskyEventStream',
                direct_access='forbid',
                args=args,
                cache=None,  # ???
                metadata={'descriptors': descriptors},
                catalog_dir=None,
                getenv=True,
                getshell=True,
                catalog=self)

    def read_canonical(self):
        for i in range(self.npartitions):
            for name, doc in self.read_partition(i):
                yield name, doc

    def read_partition(self, i):
        """Fetch one chunk of documents.
        """
        self._load()
        payload = []
        start = i * self.PARTITION_SIZE
        stop = (1 + i) * self.PARTITION_SIZE
        if start < self._offset:
            payload.extend(
                itertools.islice(
                    itertools.chain(
                        (('start', self._get_run_start()),),
                        (('descriptor', doc) for doc in self._descriptors)),
                    start,
                    stop))
        descriptor_uids = [doc['uid'] for doc in self._descriptors]
        skip = max(0, start - len(payload))
        limit = stop - start - len(payload)
        # print('start, stop, skip, limit', start, stop, skip, limit)
        if limit > 0:
            events = self._get_event_cursor(descriptor_uids=descriptor_uids,
                                            skip=skip, limit=limit)
            for event in events:
                try:
                    self.filler('event', event)
                except event_model.UnresolvableForeignKeyError as err:
                    datum_id = err.key

                    if '/' in datum_id:
                        resource_uid, _ = datum_id.split('/', 1)
                    else:
                        datum = self._get_datum(datum_id=datum_id)
                        resource_uid = datum['resource']

                    resource = self._get_resource(uid=resource_uid)
                    self.filler('resource', resource)
                    # Pre-fetch all datum for this resource.
                    for datum in self._get_datum_cursor(resource_uid=resource_uid):
                        self.filler('datum', datum)
                    # TODO -- When to clear the datum cache in filler?
                    self.filler('event', event)
                payload.append(('event', event))
            if i == self.npartitions - 1 and self._run_stop_doc is not None:
                payload.append(('stop', self._run_stop_doc))
        for _, doc in payload:
            doc.pop('_id', None)
        return payload

    def read(self):
        raise NotImplementedError(
            "Reading the RunCatalog itself is not supported. Instead read one "
            "its entries, representing individual Event Streams.")

    def to_dask(self):
        raise NotImplementedError(
            "Reading the RunCatalog itself is not supported. Instead read one "
            "its entries, representing individual Event Streams.")


class BlueskyEventStream(intake_xarray.base.DataSourceMixin):
    """
    Catalog representing one Event Stream from one Run.

    Parameters
    ----------
    get_run_start: callable
        Expected signature ``get_run_start() -> RunStart``
    stream_name : string
        Stream name, such as 'primary'.
    get_run_stop : callable
        Expected signature ``get_run_stop() -> RunStop``
    get_event_descriptors : callable
        Expected signature ``get_event_descriptors() -> List[EventDescriptors]``
    get_event_cursor : callable
        Expected signature ``get_event_cursor(descriptor_uids) -> generator``
        where ``generator`` yields Event documents
    get_resource : callable
        Expected signature ``get_resource(resource_uid) -> Resource``
    get_datum : callable
        Expected signature ``get_datum(datum_id) -> Datum``
    get_datum_cursor : callable
        Expected signature ``get_datum_cursor(resource_uid) -> generator``
        where ``generator`` yields Datum documents
    filler : event_model.Filler
    metadata : dict
        passed through to base class
    include : list, optional
        Fields ('data keys') to include. By default all are included. This
        parameter is mutually exclusive with ``exclude``.
    exclude : list, optional
        Fields ('data keys') to exclude. By default none are excluded. This
        parameter is mutually exclusive with ``include``.
    **kwargs :
        Additional keyword arguments are passed through to the base class.
    """
    container = 'xarray'
    name = 'bluesky-event-stream'
    version = '0.0.1'
    partition_access = True

    def __init__(self,
                 get_run_start,
                 stream_name,
                 get_run_stop,
                 get_event_descriptors,
                 get_event_cursor,
                 get_event_count,
                 get_resource,
                 get_datum,
                 get_datum_cursor,
                 filler,
                 metadata,
                 include=None,
                 exclude=None,
                 **kwargs):
        # self._partition_size = 10
        # self._default_chunks = 10
        self._get_run_start = get_run_start
        self._stream_name = stream_name
        self._get_event_descriptors = get_event_descriptors
        self._get_run_stop = get_run_stop
        self._get_event_cursor = get_event_cursor
        self._get_event_count = get_event_count
        self._get_resource = get_resource
        self._get_datum = get_datum
        self._get_datum_cursor = get_datum_cursor
        self.filler = filler
        self.urlpath = ''  # TODO Not sure why I had to add this.
        self._ds = None  # set by _open_dataset below
        self.include = include
        self.exclude = exclude
        super().__init__(
            metadata=metadata
        )

    def __repr__(self):
        try:
            out = (f"<Intake catalog: Stream {self._stream_name!r} "
                   f"from Run {self._get_run_start()['uid'][:8]}...>")
        except Exception as exc:
            out = f"<Intake catalog: Stream *REPR_RENDERING_FAILURE* {exc!r}>"
        return out

    def _open_dataset(self):
        self._run_stop_doc = self._get_run_stop()
        self._run_start_doc = self._get_run_start()
        self.metadata.update({'start': self._run_start_doc})
        self.metadata.update({'stop': self._run_stop_doc})
        descriptor_docs = [doc for doc in self._get_event_descriptors()
                           if doc.get('name') == self._stream_name]
        self._ds = documents_to_xarray(
            start_doc=self._run_start_doc,
            stop_doc=self._run_stop_doc,
            descriptor_docs=descriptor_docs,
            event_docs=list(self._get_event_cursor(
                [doc['uid'] for doc in descriptor_docs])),
            filler=self.filler,
            get_resource=self._get_resource,
            get_datum=self._get_datum,
            get_datum_cursor=self._get_datum_cursor,
            include=self.include,
            exclude=self.exclude)


def _transpose(in_data, keys, field):
    """Turn a list of dicts into dict of lists

    Parameters
    ----------
    in_data : list
        A list of dicts which contain at least one dict.
        All of the inner dicts must have at least the keys
        in `keys`

    keys : list
        The list of keys to extract

    field : str
        The field in the outer dict to use

    Returns
    -------
    transpose : dict
        The transpose of the data
    """
    out = {k: [None] * len(in_data) for k in keys}
    for j, ev in enumerate(in_data):
        dd = ev[field]
        for k in keys:
            out[k][j] = dd[k]
    return out


def _ft(timestamp):
    "format timestamp"
    if isinstance(timestamp, str):
        return timestamp
    # Truncate microseconds to miliseconds. Do not bother to round.
    return (datetime.fromtimestamp(timestamp)
            .strftime('%Y-%m-%d %H:%M:%S.%f'))[:-3]


def xarray_to_event_gen(data_xarr, ts_xarr, page_size):
    for start_idx in range(0, len(data_xarr['time']), page_size):
        stop_idx = start_idx + page_size
        data = {name: variable.values
                for name, variable in
                data_xarr.isel({'time': slice(start_idx, stop_idx)}).items()
                if ':' not in name}
        ts = {name: variable.values
              for name, variable in
              ts_xarr.isel({'time': slice(start_idx, stop_idx)}).items()
              if ':' not in name}
        event_page = {}
        seq_num = data.pop('seq_num')
        ts.pop('seq_num')
        uids = data.pop('uid')
        ts.pop('uid')
        event_page['data'] = data
        event_page['timestamps'] = ts
        event_page['time'] = data_xarr['time'][start_idx:stop_idx].values
        event_page['uid'] = uids
        event_page['seq_num'] = seq_num
        event_page['filled'] = {}

        yield event_page


def parse_handler_registry(handler_registry):
    """
    Parse mapping of spec name to 'import path' into mapping to class itself.

    Parameters
    ----------
    handler_registry : dict
        Values may be string 'import paths' to classes or actual classes.

    Examples
    --------
    Pass in name; get back actual class.

    >>> parse_handler_registry({'my_spec': 'package.module.ClassName'})
    {'my_spec': <package.module.ClassName>}

    """
    result = {}
    for spec, handler_str in handler_registry.items():
        if isinstance(handler_str, str):
            module_name, _, class_name = handler_str.rpartition('.')
            class_ = getattr(importlib.import_module(module_name), class_name)
        else:
            class_ = handler_str
        result[spec] = class_
    return result


intake.registry['remote-bluesky-run-catalog'] = RemoteRunCatalog
intake.container.container_map['bluesky-run-catalog'] = RemoteRunCatalog
