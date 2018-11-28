import collections
from datetime import datetime
import intake.catalog
import intake.catalog.base
import intake.catalog.local
import intake.catalog.remote
import intake.source.base
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


class MongoMetadataStoreCatalog(intake.catalog.Catalog):
    "represents one MongoDB instance"
    def __init__(self, uri, *, query=None, **kwargs):
        """
        A Catalog backed by a MongoDB with metadatastore v1 collections.

        Parameters
        ----------
        uri : string
            This URI must include a database name.
            Example: ``mongodb://localhost:27107/database_name``.
        query : dict
            Mongo query. This is used internally by the ``search`` method.
        **kwargs :
            passed up to the Catalog base class
        """
        # All **kwargs are passed up to base class. TODO: spell them out
        # explicitly.
        self._uri = uri
        self._query = query
        self._client = pymongo.MongoClient(uri)
        kwargs.setdefault('name', uri)
        try:
            # Called with no args, get_database() returns the database
            # specified in the uri --- or raises if there was none. There is no
            # public method for checking this in advance, so we just catch the
            # error.
            db = self._client.get_database()
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(
                "Invalid uri. Did you forget to include a database?") from err
        self._run_start_collection = db.get_collection('run_start')
        self._run_stop_collection = db.get_collection('run_stop')
        self._event_descriptor_collection = db.get_collection('event_descriptor')
        self._event_collection = db.get_collection('event')
        self._query = query or {}
        super().__init__(**kwargs)
        if self.metadata is None:
            self.metadata = {}

        catalog = self

        class Entries:
            "Mock the dict interface around a MongoDB query result."
            def _doc_to_entry(self, run_start_doc):
                args = dict(
                    run_start_doc=run_start_doc,
                    run_stop_collection=catalog._run_stop_collection,
                    event_descriptor_collection=catalog._event_descriptor_collection,
                    event_collection=catalog._event_collection)
                return intake.catalog.local.LocalCatalogEntry(
                    name=run_start_doc['uid'],
                    description={},  # TODO
                    driver='intake_bluesky.RunCatalog',
                    direct_access='forbid',  # ???
                    args=args,
                    cache=None,  # ???
                    parameters={},
                    metadata={},  # TODO
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
                except TypeError:
                    pass
                if isinstance(name, int):
                    if name < 0:
                        # Interpret negative N as "the Nth from last entry".
                        query = catalog._query
                        cursor = (catalog._run_start_collection.find(query)
                                .sort('time', pymongo.DESCENDING)
                                .limit(name))
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

        self._entries = Entries()

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
            uri=self._uri,
            query=query,
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options)
        cat.metadata['search'] = {'query': query, 'upstream': self.name}
        return cat


class RunCatalog(intake_xarray.base.DataSourceMixin):
    "represents one Run"
    container = 'xarray'
    name = 'run'
    version = '0.0.1'
    partition_access = True

    def __init__(self,
                 run_start_doc,
                 run_stop_collection,
                 event_descriptor_collection,
                 event_collection,
                 **kwargs):
        # All **kwargs are passed up to base class. TODO: spell them out
        # explicitly.
        self.urlpath = ''
        self._ds = None

        self._run_start_doc = run_start_doc
        self._run_stop_doc = None  # loaded in _load below
        self._run_stop_collection = run_stop_collection
        self._event_descriptor_collection = event_descriptor_collection
        self._event_collection = event_collection

        super().__init__(**kwargs)

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

    def _open_dataset(self):
        uid = self._run_start_doc['uid']
        run_stop_doc = self._run_stop_collection.find_one({'run_start': uid})
        self._run_stop_doc = run_stop_doc
        if run_stop_doc is not None:
            del run_stop_doc['_id']  # Drop internal Mongo detail.
        self.metadata.update({'start': self._run_start_doc})
        self.metadata.update({'stop': run_stop_doc})
        cursor = self._event_descriptor_collection.find(
            {'run_start': uid},
            sort=[('time', pymongo.ASCENDING)])
        # Sort descriptors like
        # {'stream_name': [descriptor1, descriptor2, ...], ...}
        streams = itertools.groupby(cursor,
                                    key=lambda d: d.get('name'))
        # Make a MongoEventStreamSource for each stream_name.
        entries = {
             stream_name: MongoEventStream(self._run_start_doc,
                                           list(event_descriptor_docs),
                                           self._event_collection,
                                           self._run_stop_collection)
             for stream_name, event_descriptor_docs in streams}
        self._ds = xarray.merge(
            [stream.read()
             for stream in entries.values()])


class MongoEventStream(intake.catalog.Catalog):
    container = 'xarray'
    name = 'foo'
    version = '0.0.1'
    partition_access = True

    def __init__(self, run_start_doc, event_descriptor_docs, event_collection,
                 run_stop_collection, metadata=None):
        self._partition_size = 10
        self._default_chunks = 10
        self._run_start_doc = run_start_doc
        self._run_stop_doc  = None
        self._event_collection = event_collection
        self._event_descriptor_docs = event_descriptor_docs
        self._stream_name = event_descriptor_docs[0].get('name')
        self._run_stop_collection = run_stop_collection
        super().__init__(
            metadata=metadata
        )
        if self.metadata is None:
            self.metadata = {}

    def __repr__(self):
        try:
            out = (f"<Intake catalog: Stream {self._stream_name!r} "
                   f"from Run {self._run_start_doc['uid'][:8]}...>")
        except Exception as exc:
            out = f"<Intake catalog: Stream *REPR_RENDERING_FAILURE* {exc}>"
        return out

    def _load(self):
        # Make a MongoEventStreamSource for each stream_name.
        self._entries = {
            field: MongoField(field,
                              self._run_start_doc,
                              self._event_descriptor_docs,
                              self._event_collection)
            for field in list(self._event_descriptor_docs[0]['data_keys'])}

    def _get_schema(self):
        return intake.source.base.Schema(
            datashape=None,
            dtype={'x': "int64", 'y': "int64"},
            shape=(None, 2),
            npartitions=2,
            extra_metadata=dict(c=3, d=4)
        )

    def read_slice(self, slice_, *, include=None, exclude=None):
        """
        Read data from a Slice of Events from this Stream into one structure.

        Parameters
        ----------
        slice_ : slice
            Example: ``slice(3, 7)``
        include : list, optional
            List of field names to include.
        exclude : list, optional
            List of field names to exclude. May only be used in ``include`` is
            blank.
        """
        self._load_metadata()
        if include and exclude:
            raise ValueError(
                "You may specify fields to include or fields to exclude, but "
                "not both.")

        if isinstance(slice_, collections.Iterable):
            first_axis = slice_[0]
        else:
            first_axis = slice_
        query = {'descriptor': {'$in': [d['uid'] for d in self._event_descriptor_docs]}}
        seq_num_filter = {}
        if first_axis.start is not None:
            seq_num_filter['$gte'] = first_axis.start
        if first_axis.stop is not None:
            seq_num_filter['$lt'] = first_axis.stop
        if first_axis.step is not None:
            # Have to think about how this interacts with repeated seq_num.
            raise NotImplementedError
        if seq_num_filter:
            query['seq_num'] = seq_num_filter
        cursor = self._event_collection.find(
            query,
            sort=[('descriptor', pymongo.DESCENDING),
                  ('time', pymongo.ASCENDING)])

        events = list(cursor)
        # Put time into a vectorized data sturcutre with suitable dimensions
        # for xarray coords.
        times = numpy.expand_dims([ev['time'] for ev in events], 0)
        # seq_nums = numpy.expand_dims([ev['seq_num'] for ev in events], 0)
        # uids = [ev['uid'] for ev in events]
        data_keys = self._event_descriptor_docs[0]['data_keys']
        if include:
            keys = list(set(data_keys) & set(include))
        elif exclude:
            keys = list(set(data_keys) - set(exclude))
        else:
            keys = list(data_keys)
        data_table = _transpose(events, keys, 'data')
        # timestamps_table = _transpose(all_events, keys, 'timestamps')
        # data_keys = descriptor['data_keys']
        # external_keys = [k for k in data_keys if 'external' in data_keys[k]]
        data_arrays = {}
        for key in keys:
            # TODO Some sim objects wrongly report 'integer'. with event-model
            # should not allow.
            SCALAR_TYPES = ('number', 'string',  'boolean', 'null', 'integer')
            if data_keys[key]['dtype'] in SCALAR_TYPES:
                data_arrays[key] = xarray.DataArray(data=data_table[key],
                                                    dims=('time',),
                                                    coords=times,
                                                    name=key)
            else:
                raise NotImplementedError
        return xarray.Dataset(data_vars=data_arrays)

    def read(self, *, include=None, exclude=None):
        """
        Read all the data from this Stream into one structure.

        Parameters
        ----------
        include : list, optional
            List of field names to include.
        exclude : list, optional
            List of field names to exclude. May only be used in ``include`` is
            blank.
        """
        return self.read_slice(slice(None), include=include, exclude=exclude)

    def read_chunked(self, chunks=None, *, include=None, exclude=None):
        """
        Read data from this Stream in chunks.

        Parameters
        ----------
        chunks : integer, optional
            Chunk size (NOT number of chunks). If None, use default specified
            by Catalog. 
        include : list, optional
            List of field names to include.
        exclude : list, optional
            List of field names to exclude. May only be used in ``include`` is
            blank.
        """
        if chunks is None:
            chunks = self._default_chunks
        for i in itertools.count():
            # Start at 1 because seq_num starts at 1 ugh....
            partition = self.read_slice(
                slice(1 + i * chunks, 1 + (i + 1) * chunks),
                include=include, exclude=exclude)
            # This is how to tell if an xarray.Dataset is empty.
            if any(any(da.shape) for da in partition.data_vars.values()):
                yield partition
            else:
                break

    def _close(self):
        pass


class MongoField(intake.source.base.DataSource):
    container = 'xarray'
    name = 'foo'
    version = '0.0.1'
    partition_access = True

    def __init__(self, field,
                 run_start_doc, event_descriptor_docs, event_collection,
                 metadata=None):
        self._partition_size = 10
        self._default_chunks = 10
        self._field = field
        self._run_start_doc = run_start_doc  # just used for __repr__
        self._event_collection = event_collection
        self._event_descriptor_docs = event_descriptor_docs
        self._stream_name = event_descriptor_docs[0].get('name')
        super().__init__(
            metadata=metadata
        )

    def __repr__(self):
        try:
            out = (f"<Intake datasource: Field {self._field!r} "
                   f"of Stream {self._stream_name!r} "
                   f"from Run {self._run_start_doc['uid'][:8]}...>")
        except Exception as exc:
            out = f"<Intake catalog: Run *REPR_RENDERING_FAILURE* {exc}>"
        return out

    def _get_schema(self):
        return intake.source.base.Schema(
            datashape=None,
            dtype={'x': "int64", 'y': "int64"},
            shape=(None, 2),
            npartitions=2,
            extra_metadata=dict(c=3, d=4)
        )

    def read_slice(self, slice_):
        """
        Read a slice of data from this field.

        Parameters
        ----------
        slice_ : slice
            Example: ``slice(3, 7)``
        """
        if isinstance(slice_, collections.Iterable):
            first_axis = slice_[0]
        else:
            first_axis = slice_
        query = {'descriptor': {'$in': [d['uid'] for d in self._event_descriptor_docs]}}
        seq_num_filter = {}
        if first_axis.start is not None:
            seq_num_filter['$gte'] = first_axis.start
        if first_axis.stop is not None:
            seq_num_filter['$lt'] = first_axis.stop
        if first_axis.step is not None:
            # Have to think about how this interacts with repeated seq_num.
            raise NotImplementedError
        if seq_num_filter:
            query['seq_num'] = seq_num_filter
        cursor = self._event_collection.find(
            query,
            sort=[('descriptor', pymongo.DESCENDING),
                  ('time', pymongo.ASCENDING)])

        events = list(cursor)
        # seq_nums = [ev['seq_num'] for ev in events]
        times = numpy.expand_dims([ev['time'] for ev in events], 0)
        data_keys = self._event_descriptor_docs[0]['data_keys']
        keys = (self._field,)
        data_table = _transpose(events, keys, 'data')
        # timestamps_table = _transpose(all_events, keys, 'timestamps')
        # data_keys = descriptor['data_keys']
        # external_keys = [k for k in data_keys if 'external' in data_keys[k]]
        data_arrays = {}
        for key in keys:
            # TODO Some sim objects wrongly report 'integer'. with event-model
            # should not allow.
            SCALAR_TYPES = ('number', 'string',  'boolean', 'null', 'integer')
            if data_keys[key]['dtype'] in SCALAR_TYPES:
                data_arrays[key] = xarray.DataArray(data=data_table[key],
                                                    dims=('time',),
                                                    coords=times,
                                                    name=key)
            else:
                raise NotImplementedError
        return xarray.Dataset(data_vars=data_arrays)

    def read_chunked(self, chunks=None):
        """
        Read all the data from this field in chunks.

        Parameters
        ----------
        chunks : integer, optional
            Chunk size (NOT number of chunks). If None, use default specified
            by Catalog. 
        """
        if chunks is None:
            chunks = self._default_chunks
        for i in itertools.count():
            # Start at 1 because seq_num starts at 1 ugh....
            partition = self.read_slice(
                slice(1 + i * chunks, 1 + (i + 1) * chunks))
            if len(partition):
                yield partition
            else:
                break

    def read(self):
        """
        Read all the data from this field into one structure.
        """
        return self.read_slice(slice(None))

    def _close(self):
        pass


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


class MongoInsertCallback:
    """
    This is a replacmenet for db.insert.
    """
    def __init__(self, uri):
        self._uri = uri
        self._client = pymongo.MongoClient(uri)
        try:
            # Called with no args, get_database() returns the database
            # specified in the uri --- or raises if there was none. There is no
            # public method for checking this in advance, so we just catch the
            # error.
            db = self._client.get_database()
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(
                "Invalid uri. Did you forget to include a database?") from err

        self._run_start_collection = db.get_collection('run_start')
        self._run_stop_collection = db.get_collection('run_stop')
        self._event_descriptor_collection = db.get_collection('event_descriptor')
        self._event_collection = db.get_collection('event')

    def __call__(self, name, doc):
        getattr(self, name)(doc)

    def start(self, doc):
        self._run_start_collection.insert_one(doc)

    def descriptor(self, doc):
        self._event_descriptor_collection.insert_one(doc)

    def event(self, doc):
        self._event_collection.insert_one(doc)

    def stop(self, doc):
        self._run_stop_collection.insert_one(doc)
