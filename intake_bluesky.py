import ast
import dask
import dask.bag
import collections
from datetime import datetime
import event_model
import importlib
import intake
import intake.catalog
import intake.catalog.base
import intake.catalog.local
import intake.catalog.remote
import intake.container.base
import intake.container.semistructured
import intake.source.base
from intake.compat import unpack_kwargs
import intake_xarray.base
import itertools
import msgpack
import numpy
import pandas
import pymongo
import pymongo.errors
import requests
from requests.compat import urljoin, urlparse
import time
import xarray


class FacilityCatalog(intake.catalog.Catalog):
    "spans multiple MongoDB instances"
    ...


def parse_handler_registry(handler_registry):
    result = {}
    for spec, handler_str in handler_registry.items():
        module_name, _, class_name = handler_str.rpartition('.')
        result[spec] = getattr(importlib.import_module(module_name), class_name)
    return result


class MongoMetadataStoreCatalog(intake.catalog.Catalog):
    def __init__(self, metadatastore_uri, asset_registry_uri, *,
                 handler_registry=None, query=None, **kwargs):
        """
        Insert documents into MongoDB using layout v1.

        This layout uses a separate Mongo collection per document type and a
        separate Mongo document for each logical document.

        Note that this Seralizer does not share the standard Serializer
        name or signature common to suitcase packages because it can only write
        via pymongo, not to an arbitrary user-provided buffer.
        """
        name = 'mongo_metadatastore'

        self._metadatastore_uri = metadatastore_uri
        self._asset_registry_uri = asset_registry_uri
        metadatastore_client = pymongo.MongoClient(metadatastore_uri)
        asset_registry_client = pymongo.MongoClient(asset_registry_uri)
        self._metadatastore_client = metadatastore_client
        self._asset_registry_client = asset_registry_client

        try:
            # Called with no args, get_database() returns the database
            # specified in the client's uri --- or raises if there was none.
            # There is no public method for checking this in advance, so we
            # just catch the error.
            mds_db = self._metadatastore_client.get_database()
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(
                f"Invalid metadatastore_client: {metadatastore_client} "
                f"Did you forget to include a database?") from err
        try:
            assets_db = self._asset_registry_client.get_database()
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(
                f"Invalid asset_registry_client: {asset_registry_client} "
                f"Did you forget to include a database?") from err

        self._run_start_collection = mds_db.get_collection('run_start')
        self._run_stop_collection = mds_db.get_collection('run_stop')
        self._event_descriptor_collection = mds_db.get_collection('event_descriptor')
        self._event_collection = mds_db.get_collection('event')

        self._resource_collection = assets_db.get_collection('resource')
        self._datum_collection = assets_db.get_collection('datum')

        self._query = query or {}
        if handler_registry is None:
            handler_registry = {}
        parsed_handler_registry = parse_handler_registry(handler_registry)
        self.filler = event_model.Filler(parsed_handler_registry)
        super().__init__(**kwargs)
        if self.metadata is None:
            self.metadata = {}

        catalog = self

        class Entries:
            "Mock the dict interface around a MongoDB query result."
            def _doc_to_entry(self, run_start_doc):
                uid = run_start_doc['uid']
                run_stop_doc = catalog._run_stop_collection.find_one({'run_start': uid})
                if run_stop_doc is not None:
                    del run_stop_doc['_id']  # Drop internal Mongo detail.
                entry_metadata = {'start': run_start_doc,
                                  'stop': run_stop_doc}
                args = dict(
                    run_start_doc=run_start_doc,
                    run_stop_collection=catalog._run_stop_collection,
                    event_descriptor_collection=catalog._event_descriptor_collection,
                    event_collection=catalog._event_collection,
                    resource_collection=catalog._resource_collection,
                    datum_collection=catalog._datum_collection,
                    filler=catalog.filler)
                return intake.catalog.local.LocalCatalogEntry(
                    name=run_start_doc['uid'],
                    description={},  # TODO
                    driver='intake_bluesky.RunCatalog',
                    direct_access='forbid',  # ???
                    args=args,
                    cache=None,  # ???
                    parameters=[],
                    metadata=entry_metadata,
                    catalog_dir=None,
                    getenv=True,
                    getshell=True,
                    catalog=catalog)

            def __iter__(self):
                yield from self.keys()

            def keys(self):
                cursor = catalog._run_start_collection.find(
                    catalog._query, sort=[('time', pymongo.DESCENDING)])
                for run_start_doc in cursor:
                    yield run_start_doc['uid']

            def values(self):
                cursor = catalog._run_start_collection.find(
                    catalog._query, sort=[('time', pymongo.DESCENDING)])
                for run_start_doc in cursor:
                    del run_start_doc['_id']  # Drop internal Mongo detail.
                    yield self._doc_to_entry(run_start_doc)

            def items(self):
                cursor = catalog._run_start_collection.find(
                    catalog._query, sort=[('time', pymongo.DESCENDING)])
                for run_start_doc in cursor:
                    del run_start_doc['_id']  # Drop internal Mongo detail.
                    yield run_start_doc['uid'], self._doc_to_entry(run_start_doc)

            def __getitem__(self, name):
                # If this came from a client, we might be getting '-1'.
                try:
                    name = int(name)
                except ValueError:
                    pass
                if isinstance(name, int):
                    if name < 0:
                        # Interpret negative N as "the Nth from last entry".
                        query = catalog._query
                        cursor = (catalog._run_start_collection.find(query)
                                .sort('time', pymongo.DESCENDING) .limit(name))
                        *_, run_start_doc = cursor
                    else:
                        # Interpret positive N as
                        # "most recent entry with scan_id == N".
                        query = {'$and': [catalog._query, {'scan_id': name}]}
                        cursor = (catalog._run_start_collection.find(query)
                                .sort('time', pymongo.DESCENDING)
                                .limit(1))
                        run_start_doc, = cursor
                else:
                    query = {'$and': [catalog._query, {'uid': name}]}
                    run_start_doc = catalog._run_start_collection.find_one(query)
                if run_start_doc is None:
                    raise KeyError(name)
                del run_start_doc['_id']  # Drop internal Mongo detail.
                return self._doc_to_entry(run_start_doc)

            def __contains__(self, key):
                # Avoid iterating through all entries.
                try:
                    self[key]
                except KeyError:
                    return False
                else:
                    return True

        self._entries = Entries()
        self._schema = {}  # TODO This is cheating, I think.

    def _close(self):
        self._client.close()

    def search(self, query):
        """
        Return a new Catalog with a subset of the entries in this Catalog.

        Parameters
        ----------
        query : dict
            MongoDB query.
        """
        if self._query:
            query = {'$and': [self._query, query]}
        cat = MongoMetadataStoreCatalog(
            metadatastore_uri=self._metadatastore_uri,
            asset_registry_uri=self._asset_registry_uri,
            query=query,
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options)
        cat.metadata['search'] = {'query': query, 'upstream': self.name}
        return cat


class RemoteRunCatalog(intake.catalog.base.RemoteCatalog):
    """
    Client-side proxy to a RunCatalog on the server.
    """
    name = 'bluesky-run-catalog'

    def __init__(self, url, headers, name, parameters, metadata=None, **kwargs):
        """

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
        super().__init__(url=url, headers=headers, name=name,
                metadata=metadata, **kwargs)
        self.url = url
        self.name = name
        self.parameters = parameters
        self.headers = headers
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
                                **self.headers)
            req.raise_for_status()
            response = msgpack.unpackb(req.content, **unpack_kwargs)
            self._parse_open_response(response)

    def _parse_open_response(self, response):
        self.npartitions = response['npartitions']
        self.metadata = response['metadata']
        self._schema = intake.source.base.Schema(datashape=None, dtype=None,
                              shape=self.shape,
                              npartitions=self.npartitions,
                              metadata=self.metadata)
        self._source_id = response['source_id']

    def _load_metadata(self):
        if self.bag is None:
            self.parts = [dask.delayed(intake.container.base.get_partition)(
                self.url, self.headers, self._source_id, self.container, i
            )
                          for i in range(self.npartitions)]
            self.bag = dask.bag.from_delayed(self.parts)
        return self._schema

    def _get_partition(self, i):
        self._load_metadata()
        return self.parts[i].compute()

    def read(self):
        self._load_metadata()
        return self.bag.compute()

    def to_dask(self):
        self._load_metadata()
        return self.bag

    def _close(self):
        self.bag = None

    def read_canonical(self):
        for i in range(self.npartitions):
            for name, doc in self._get_partition(i):
                yield name, doc


class RunCatalog(intake.catalog.Catalog):
    "represents one Run"
    container = 'bluesky-run-catalog'
    version = '0.0.1'
    partition_access = True
    PARTITION_SIZE = 100

    def __init__(self,
                 run_start_doc,
                 run_stop_collection,
                 event_descriptor_collection,
                 event_collection,
                 resource_collection,
                 datum_collection,
                 filler,
                 **kwargs):
        # All **kwargs are passed up to base class. TODO: spell them out
        # explicitly.
        self.urlpath = ''  # TODO Not sure why I had to add this.

        self._run_start_doc = run_start_doc
        self._run_stop_collection = run_stop_collection
        self._event_descriptor_collection = event_descriptor_collection
        self._event_collection = event_collection
        self._resource_collection = resource_collection
        self._datum_collection = datum_collection
        self.filler = filler
        # loaded in _load below
        super().__init__(**kwargs)
        
        # Count the total number of documents in this run.
        uid = self._run_start_doc['uid']
        run_stop_doc = self._run_stop_collection.find_one({'run_start': uid})
        if run_stop_doc is not None:
            del run_stop_doc['_id']  # Drop internal Mongo detail.
        self._run_stop_doc = run_stop_doc
        cursor = self._event_descriptor_collection.find(
            {'run_start': uid},
            sort=[('time', pymongo.ASCENDING)])
        self._descriptors = list(cursor)
        self._offset = len(self._descriptors) + 1
        self.metadata.update({'start': self._run_start_doc})
        self.metadata.update({'stop': run_stop_doc})

        count = 1
        descriptor_uids = [doc['uid'] for doc in self._descriptors]
        count += len(descriptor_uids)
        query = {'descriptor': {'$in': descriptor_uids}}
        count += self._event_collection.find(query).count()
        count += (self.run_stop_doc is not None)
        self.npartitions = int(numpy.ceil(count / self.PARTITION_SIZE))

        self._schema = intake.source.base.Schema(
            datashape=None,
            dtype=None,
            shape=(count,),
            npartitions=self.npartitions,
            metadata=self.metadata)

    def __repr__(self):
        try:
            start = self._run_start_doc
            stop = self._run_stop_doc or {}
            out = (f"<Intake catalog: Run {start['uid'][:8]}...>\n"
                   f"  {_ft(start['time'])} -- {_ft(stop.get('time', '?'))}\n")
                   # f"  Streams:\n")
            # for stream_name in self:
            #     out += f"    * {stream_name}\n"
        except Exception as exc:
            out = f"<Intake catalog: Run *REPR_RENDERING_FAILURE* {exc}>"
        return out

    def _load(self):
        uid = self._run_start_doc['uid']
        cursor = self._event_descriptor_collection.find(
            {'run_start': uid},
            sort=[('time', pymongo.ASCENDING)])
        # Sort descriptors like
        # {'stream_name': [descriptor1, descriptor2, ...], ...}
        streams = itertools.groupby(cursor,
                                    key=lambda d: d.get('name'))

        # Make a MongoEventStreamSource for each stream_name.
        for stream_name, event_descriptor_docs in streams:
            args = {'run_start_doc': self._run_start_doc,
                    'event_descriptor_docs': list(event_descriptor_docs),
                    'event_collection': self._event_collection,
                    'run_stop_collection': self._run_stop_collection,
                    'resource_collection': self._resource_collection,
                    'datum_collection': self._datum_collection,
                    'filler': self.filler,
                    'metadata': {'descriptors': list(event_descriptor_docs)},
                    'include': '{{ include }}',
                    'exclude': '{{ exclude }}'}
            self._entries[stream_name] = intake.catalog.local.LocalCatalogEntry(
                name=stream_name,
                description={},  # TODO
                driver='intake_bluesky.MongoEventStream',
                direct_access='forbid',
                args=args,
                cache=None,  # ???
                parameters=[_INCLUDE_PARAMETER, _EXCLUDE_PARAMETER],
                metadata={'descriptors': list(event_descriptor_docs)},
                catalog_dir=None,
                getenv=True,
                getshell=True,
                catalog=self)

    def read_canonical(self):
        ...

        return self._descriptors

    @property
    def run_stop_doc(self):
        if self._run_stop_doc is None:
            run_stop_doc = self._run_stop_collection.find_one({'run_start': uid})
            if run_stop_doc is not None:
                del run_stop_doc['_id']  # Drop internal Mongo detail.
            self._run_stop_doc = run_stop_doc
        return self._run_stop_doc

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
                        (('start', self._run_start_doc),),
                        (('descriptor', doc) for doc in self._descriptors)),
                    start,
                    stop))
        descriptor_uids = [doc['uid'] for doc in self._descriptors]
        skip = max(0, start - len(payload))
        limit = stop - start - len(payload)
        # print('start, stop, skip, limit', start, stop, skip, limit)
        if limit > 0:
            events = [doc
                      for doc in self._event_collection
                          .find({'descriptor': {'$in': descriptor_uids}},
                                sort=[('time', pymongo.ASCENDING)])
                          .skip(skip)
                          .limit(limit)]
            for event in events:
                try:
                    self.filler('event', event)
                except event_model.UnresolvableForeignKeyError as err:
                    datum = self._datum_collection.find_one(
                        {'datum_id': err.key})
                    resource = self._resource_collection.find_one(
                        {'uid': datum['resource']})
                    self.filler('resource', resource)
                    # Pre-fetch all datum for this resource.
                    for datum in self._datum_collection.find(
                            {'resource': datum['resource']}):
                        self.filler('datum', datum)
                    # TODO -- When to clear the datum cache in filler?
                    self.filler('event', event)
                payload.append(('event', event))
            if i == self.npartitions - 1 and self._run_stop_doc is not None:
                payload.append(('stop', self.run_stop_doc))
        for _, doc in payload:
            doc.pop('_id', None)
        return payload


_EXCLUDE_PARAMETER = intake.catalog.local.UserParameter(
    name='exclude',
    description="fields to exclude",
    type='list',
    default=None)
_INCLUDE_PARAMETER = intake.catalog.local.UserParameter(
    name='include',
    description="fields to explicitly include at exclusion of all others",
    type='list',
    default=None)


class MongoEventStream(intake_xarray.base.DataSourceMixin):
    container = 'xarray'
    name = 'event-stream'
    version = '0.0.1'
    partition_access = True

    def __init__(self, run_start_doc, event_descriptor_docs, event_collection,
                 run_stop_collection, resource_collection, datum_collection,
                 filler, metadata, include, exclude):
        # self._partition_size = 10
        # self._default_chunks = 10
        self.urlpath = ''  # TODO Not sure why I had to add this.
        self._run_start_doc = run_start_doc
        self._run_stop_doc  = None
        self._event_collection = event_collection
        self._event_descriptor_docs = event_descriptor_docs
        self._stream_name = event_descriptor_docs[0].get('name')
        self._run_stop_collection = run_stop_collection
        self._resource_collection = resource_collection
        self._datum_collection = datum_collection
        self.filler = filler
        self._ds = None  # set by _open_dataset below
        # TODO Is there a more direct way to get non-string UserParameters in?
        self.include = ast.literal_eval(include)
        self.exclude = ast.literal_eval(exclude)
        super().__init__(
            metadata=metadata
        )

    def __repr__(self):
        try:
            out = (f"<Intake catalog: Stream {self._stream_name!r} "
                   f"from Run {self._run_start_doc['uid'][:8]}...>")
        except Exception as exc:
            out = f"<Intake catalog: Stream *REPR_RENDERING_FAILURE* {exc}>"
        return out

    def _open_dataset(self):
        uid = self._run_start_doc['uid']
        run_stop_doc = self._run_stop_collection.find_one({'run_start': uid})
        self._run_stop_doc = run_stop_doc
        if run_stop_doc is not None:
            del run_stop_doc['_id']  # Drop internal Mongo detail.
        self.metadata.update({'start': self._run_start_doc})
        self.metadata.update({'stop': run_stop_doc})
        # TODO pass this in from BlueskyEntry
        slice_ = slice(None)

        if isinstance(slice_, collections.Iterable):
            first_axis = slice_[0]
        else:
            first_axis = slice_
        seq_num_filter = {}
        if first_axis.start is not None:
            seq_num_filter['$gte'] = first_axis.start
        if first_axis.stop is not None:
            seq_num_filter['$lt'] = first_axis.stop
        if first_axis.step is not None:
            # Have to think about how this interacts with repeated seq_num.
            raise NotImplementedError

        # Data keys must not change within one stream, so we can safely sample
        # just the first Event Descriptor.
        data_keys = self._event_descriptor_docs[0]['data_keys']
        if self.include:
            keys = list(set(data_keys) & set(self.include))
        elif self.exclude:
            keys = list(set(data_keys) - set(self.exclude))
        else:
            keys = list(data_keys)

        # Collect a Dataset for each descriptor. Merge at the end.
        datasets = []
        for descriptor in self._event_descriptor_docs:
            # Fetch (relevant range of) Event data and transpose rows -> cols.
            query = {'descriptor': descriptor['uid']}
            if seq_num_filter:
                query['seq_num'] = seq_num_filter
            cursor = self._event_collection.find(
                query,
                sort=[('descriptor', pymongo.DESCENDING),
                    ('time', pymongo.ASCENDING)])
            events = list(cursor)
            if any(data_keys[key].get('external') for key in keys):
                for event in events:
                    try:
                        self.filler('event', event)
                    except event_model.UnresolvableForeignKeyError as err:
                        datum = self._datum_collection.find_one(
                            {'datum_id': err.key})
                        resource = self._resource_collection.find_one(
                            {'uid': datum['resource']})
                        self.filler('resource', resource)
                        # Pre-fetch all datum for this resource.
                        for datum in self._datum_collection.find(
                                {'resource': datum['resource']}):
                            self.filler('datum', datum)
                        # TODO -- When to clear the datum cache in filler?
                        self.filler('event', event)
            times = [ev['time'] for ev in events]
            seq_nums = [ev['seq_num'] for ev in events]
            uids = [ev['uid'] for ev in events]
            self._data_or_timestamps = 'data'  # HACK FOR NOW
            data_table = _transpose(events, keys, self._data_or_timestamps)
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
                else:
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
                if self.include:
                    keys = {k: v for k, v in scoped_data_keys.items()
                            if v in self.include}
                elif self.exclude:
                    keys = {k: v for k, v in scoped_data_keys.items()
                            if v not in self.include}
                else:
                    keys = scoped_data_keys
                for key, scoped_key in keys.items():
                    field_metadata = data_keys[key]
                    # Verify the actual ndim by looking at the data.
                    ndim = numpy.asarray(config[self._data_or_timestamps][key]).ndim
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
                    else:
                        data_arrays[scoped_key] = xarray.DataArray(
                            # TODO Once we know we have one Event Descriptor
                            # per stream we can be more efficient about this.
                            data=numpy.tile(config[self._data_or_timestamps][key],
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
        self._ds = xarray.merge(datasets)


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


intake.registry['remote-bluesky-run-catalog'] = RemoteRunCatalog
intake.container.container_map['bluesky-run-catalog'] = RemoteRunCatalog
intake.registry['mongo_metadatastore'] = MongoMetadataStoreCatalog
