import collections
import copy
import entrypoints
import event_model
from datetime import datetime
import dask
import dask.bag
from dask import array
import functools
import heapq
import importlib
import itertools
import intake.catalog.base
import intake.catalog.local
import errno
import intake.container.base
from intake.compat import unpack_kwargs
import msgpack
import requests
from requests.compat import urljoin
import numpy
import os
import warnings
import xarray

from .intake_xarray_core.base import DataSourceMixin
from .intake_xarray_core.xarray_container import RemoteXarray
from collections import deque


class PartitionIndexError(IndexError):
    ...


class Entry(intake.catalog.local.LocalCatalogEntry):
    def __init__(self, **kwargs):
        # This might never come up, but just to be safe....
        if 'entry' in kwargs['args']:
            raise TypeError("The args cannot contain 'entry'. It is reserved.")
        super().__init__(**kwargs)

    def _create_open_args(self, user_parameters):
        plugin, open_args = super()._create_open_args(user_parameters)
        # Inject self into arguments passed to instanitate the driver. This
        # enables the driver instance to know which Entry created it.
        open_args['entry'] = self
        return plugin, open_args

def tail(filename, n=1, bsize=2048):
    """
    Returns a generator with the last n lines of a file.

    Thanks to Martijn Pieters for this solution:
    https://stackoverflow.com/a/12295054/6513183

    Parameters
    ----------
    filename : string
    n: int
        number of lines
    bsize: int
        seek step size
    Returns
    -------
    line : generator
    """

    # get newlines type, open in universal mode to find it
    with open(filename, 'r', newline=None) as hfile:
        if not hfile.readline():
            return  # empty, no point
        sep = hfile.newlines  # After reading a line, python gives us this
    assert isinstance(sep, str), 'multiple newline types found, aborting'

    # find a suitable seek position in binary mode
    with open(filename, 'rb') as hfile:
        hfile.seek(0, os.SEEK_END)
        linecount = 0
        pos = 0

        while linecount <= n + 1:
            # read at least n lines + 1 more; we need to skip a partial line later on
            try:
                hfile.seek(-bsize, os.SEEK_CUR)           # go backwards
                linecount += hfile.read(bsize).count(sep.encode())  # count newlines
                hfile.seek(-bsize, os.SEEK_CUR)           # go back again
            except IOError as e:
                if e.errno == errno.EINVAL:
                    # Attempted to seek past the start, can't go further
                    bsize = hfile.tell()
                    hfile.seek(0, os.SEEK_SET)
                    pos = 0
                    linecount += hfile.read(bsize).count(sep.encode())
                    break
                raise  # Some other I/O exception, re-raise
            pos = hfile.tell()

    # Re-open in text mode
    with open(filename, 'r') as hfile:
        hfile.seek(pos, os.SEEK_SET)  # our file position from above
        for line in hfile:
            # We've located n lines *or more*, so skip if needed
            if linecount > n:
                linecount -= 1
                continue
            # The rest we yield
            yield line.rstrip()


def to_event_pages(get_event_cursor, page_size):
    """
    Decorator that changes get_event_cursor to get_event_pages.

    get_event_cursor yields events, get_event_pages yields event_pages.

    Parameters
    ----------
    get_event_cursor : function

    Returns
    -------
    get_event_pages : function
    """

    @functools.wraps(get_event_cursor)
    def get_event_pages(*args, **kwargs):
        event_cursor = get_event_cursor(*args, **kwargs)
        while True:
            result = list(itertools.islice(event_cursor, page_size))
            if result:
                yield event_model.pack_event_page(*result)
            else:
                break

    return get_event_pages


def to_datum_pages(get_datum_cursor, page_size):
    """
    Decorator that changes get_datum_cursor to get_datum_pages.

    get_datum_cursor yields datum, get_datum_pages yields datum_pages.

    Parameters
    ----------
    get_datum_cursor : function

    Returns
    -------
    get_datum_pages : function
    """

    @functools.wraps(get_datum_cursor)
    def get_datum_pages(*args, **kwargs):
        datum_cursor = get_datum_cursor(*args, **kwargs)
        while True:
            result = list(itertools.islice(datum_cursor, page_size))
            if result:
                yield event_model.pack_datum_page(*result)
            else:
                break

    return get_datum_pages


def flatten_event_page_gen(gen):
    """
    Converts an event_page generator to an event generator.

    Parameters
    ----------
    gen : generator

    Returns
    -------
    event_generator : generator
    """
    for page in gen:
        yield from event_model.unpack_event_page(page)


def interlace_event_pages(*gens):
    """
    Take event_page generators and interlace their results by timestamp.
    This is a modification of https://github.com/bluesky/databroker/pull/378/

    Parameters
    ----------
    gens : generators
        Generators of (name, dict) pairs where the dict contains a 'time' key.
    Yields
    ------
    val : tuple
        The next (name, dict) pair in time order

    """
    iters = [iter(g) for g in gens]
    heap = []

    def safe_next(indx):
        try:
            val = next(iters[indx])
        except StopIteration:
            return
        heapq.heappush(heap, (val['time'][0], indx, val))
    for i in range(len(iters)):
        safe_next(i)
    while heap:
        _, indx, val = heapq.heappop(heap)
        yield val
        safe_next(indx)


def interlace_event_page_chunks(*gens, chunk_size):
    """
    Take event_page generators and interlace their results by timestamp.

    This is a modification of https://github.com/bluesky/databroker/pull/378/

    Parameters
    ----------
    gens : generators
        Generators of (name, dict) pairs where the dict contains a 'time' key.
    chunk_size : integer
        Size of pages to yield
    Yields
    ------
    val : tuple
        The next (name, dict) pair in time order

    """
    iters = [iter(event_model.rechunk_event_pages(g, chunk_size)) for g in gens]
    heap = []

    def safe_next(indx):
        try:
            val = next(iters[indx])
        except StopIteration:
            return
        heapq.heappush(heap, (val['time'][0], indx, val))
    for i in range(len(iters)):
        safe_next(i)
    while heap:
        _, indx, val = heapq.heappop(heap)
        yield val


def interlace(*gens, strict_order=True):
    """
    Take event_page generators and interlace their results by timestamp.

    This is a modification of https://github.com/bluesky/databroker/pull/378/

    Parameters
    ----------
    gens : generators
        Generators of (name, dict) pairs where the dict contains a 'time' key.
    strict_order : bool, optional
        documents are strictly yielded in ascending time order. Defaults to
        True.
    Yields
    ------
    val : tuple
        The next (name, dict) pair in time order

    """
    iters = [iter(g) for g in gens]
    heap = []
    fifo = deque()

    # Gets the next event/event_page from the iterator iters[index], while
    # appending documents that are not events/event_pages to the other_docs
    # deque.
    def get_next(index):
        while True:
            try:
                name, doc = next(iters[index])
            except StopIteration:
                return
            if name == 'event':
                heapq.heappush(heap, (doc['time'], index, ('event', doc)))
                return
            if name == 'event_page':
                if strict_order:
                    for event in event_model.unpack_event_page(doc):
                        heapq.heappush(heap, (event['time'], index, ('event', event)))
                    return
                else:
                    heapq.heappush(heap, (doc['time'][0], index, (name, doc)))
                    return
            else:
                if name not in ['start', 'stop']:
                    fifo.append((name, doc))

    # Put the next event/event_page from each generator in the heap.
    for i in range(len(iters)):
        get_next(i)

    # First yield docs that are not events or event pages from the fifo queue,
    # and then yield from the heap.  We can improve this by keeping a count of
    # the number of documents from each stream in the heap, and only calling
    # get_next when the count is 0. As is the heap could potentially get very
    # large.
    while heap:
        while fifo:
            yield fifo.popleft()
        _, index, doc = heapq.heappop(heap)
        yield doc
        get_next(index)

    # Yield any remaining items in the fifo queue.
    while fifo:
        yield fifo.popleft()


def unfilled_partitions(start, descriptors, resources, stop, datum_gens,
                        event_gens, partition_size):
    """
    Return a Bluesky run, in order, packed into partitions.

    Parameters
    ----------
    start : dict
        Bluesky run_start document
    descriptors: list
        List of Bluesky descriptor documents
    resources: list
        List of Bluesky resource documents
    stop: dict
        Bluesky run_stop document
    datum_gens : generators
        Generators of datum_pages.
    event_gens : generators
        Generators of (name, dict) pairs where the dict contains a 'time' key.
    partition_size : integer
        Size of partitions to yield
    chunk_size : integer
        Size of pages to yield

    Yields
    ------
    partition : list
        List of lists of (name, dict) pair in time order
    """
    # The first partition is the "header"
    yield ([('start', start)]
          + [('descriptor', doc) for doc in descriptors]
          + [('resource', doc) for doc in resources])

    # Use rechunk datum pages to make them into pages of size "partition_size"
    # and yield one page per partition.
    for datum_gen in datum_gens:
        yield [('datum_page', datum_page) for datum_page in
               event_model.rechunk_datum_pages(datum_gen, partition_size)]

    # Rechunk the event pages and interlace them in timestamp order, then pack
    # them into a partition.
    count = 0
    partition = []
    for event_page in interlace_event_pages(*event_gens):
        partition.append(('event_page', event_page))
        count += 1
        if count == partition_size:
            yield partition
            count = 0
            partition = []

    # Add the stop document onto the last partition.
    partition.append(('stop', stop))
    yield partition


def documents_to_xarray(*, start_doc, stop_doc, descriptor_docs,
                        get_event_pages, filler, get_resource,
                        lookup_resource_for_datum, get_datum_pages,
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
    filler : event_model.Filler
    get_resource : callable
        Expected signature ``get_resource(resource_uid) -> Resource``
    lookup_resource_for_datum : callable
        Expected signature ``lookup_resource_for_datum(datum_id) -> resource_uid``
    get_datum_pages : callable
        Expected signature ``get_datum_pages(resource_uid) -> generator``
        where ``generator`` yields datum_page documents
    get_event_pages : callable
        Expected signature ``get_event_pages(descriptor_uid) -> generator``
        where ``generator`` yields event_page documents
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
    if descriptor_docs:
        data_keys = descriptor_docs[0]['data_keys']
        if include:
            keys = list(set(data_keys) & set(include))
        elif exclude:
            keys = list(set(data_keys) - set(exclude))
        else:
            keys = list(data_keys)

    # Collect a Dataset for each descriptor. Merge at the end.
    dim_counter = itertools.count()
    datasets = []
    for descriptor in descriptor_docs:
        events = list(flatten_event_page_gen(get_event_pages(descriptor['uid'])))
        if not events:
            continue
        if any(data_keys[key].get('external') for key in keys):
            filler('descriptor', descriptor)
            filled_events = []
            for event in events:
                try:
                    _, filled_event = filler('event', event)
                    filled_events.append(filled_event)
                except event_model.UnresolvableForeignKeyError as err:
                    datum_id = err.key
                    resource_uid = lookup_resource_for_datum(datum_id)
                    resource = get_resource(resource_uid)
                    filler('resource', resource)
                    # Pre-fetch all datum for this resource.
                    for datum_page in get_datum_pages(resource_uid):
                        filler('datum_page', datum_page)
                    # TODO -- When to clear the datum cache in filler?
                    _, filled_event = filler('event', event)
                    filled_events.append(filled_event)
        else:
            filled_events = events
        times = [ev['time'] for ev in events]
        seq_nums = [ev['seq_num'] for ev in events]
        uids = [ev['uid'] for ev in events]
        data_table = _transpose(filled_events, keys, 'data')
        # external_keys = [k for k in data_keys if 'external' in data_keys[k]]

        # Collect a DataArray for each field in Event, each field in
        # configuration, and 'seq_num'. The Event 'time' will be the
        # default coordinate.
        data_arrays = {}

        # Make DataArrays for Event data.
        for key in keys:
            field_metadata = data_keys[key]
            # if the EventDescriptor doesn't provide names for the
            # dimensions (it's optional) use the same default dimension
            # names that xarray would.
            try:
                dims = tuple(field_metadata['dims'])
            except KeyError:
                ndim = len(field_metadata['shape'])
                dims = tuple(f'dim_{next(dim_counter)}' for _ in range(ndim))
            data_arrays[key] = xarray.DataArray(
                data=data_table[key],
                dims=('time',) + dims,
                coords={'time': times},
                name=key)

        # Make DataArrays for configuration data.
        for object_name, config in descriptor.get('configuration', {}).items():
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
                field_metadata = data_keys[key]
                ndim = len(field_metadata['shape'])
                # if the EventDescriptor doesn't provide names for the
                # dimensions (it's optional) use the same default dimension
                # names that xarray would.
                try:
                    dims = tuple(field_metadata['dims'])
                except KeyError:
                    dims = tuple(f'dim_{next(dim_counter)}' for _ in range(ndim))
                data_arrays[scoped_key] = xarray.DataArray(
                    # TODO Once we know we have one Event Descriptor
                    # per stream we can be more efficient about this.
                    data=numpy.tile(config['data'][key],
                                    (len(times),) + ndim * (1,) or 1),
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


class RemoteBlueskyRun(intake.catalog.base.RemoteCatalog):
    """
    Catalog representing one Run.

    This is a client-side proxy to a BlueskyRun stored on a remote server.

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
    name = 'bluesky-run'

    def __init__(self, url, http_args, name, parameters, metadata=None, **kwargs):
        self.url = url
        self.name = name
        self.parameters = parameters
        self.http_args = http_args
        self._source_id = None
        self.metadata = metadata or {}
        response = self._get_source_id()
        self.bag = None
        self._source_id = response['source_id']
        super().__init__(url=url, http_args=http_args, name=name,
                         metadata=metadata,
                         source_id=self._source_id)
        self.npartitions = response['npartitions']
        self.metadata = response['metadata']
        self._schema = intake.source.base.Schema(
            datashape=None, dtype=None,
            shape=self.shape,
            npartitions=self.npartitions,
            metadata=self.metadata)

    def _get_source_id(self):
        if self._source_id is None:
            payload = dict(action='open', name=self.name,
                           parameters=self.parameters)
            req = requests.post(urljoin(self.url, '/v1/source'),
                                data=msgpack.packb(payload, use_bin_type=True),
                                **self.http_args)
            req.raise_for_status()
            response = msgpack.unpackb(req.content, **unpack_kwargs)
            return response

    def _load_metadata(self):
        return self._schema

    def _get_partition(self, partition):
        return intake.container.base.get_partition(self.url, self.http_args,
                                                   self._source_id, self.container,
                                                   partition)

    def read(self):
        raise NotImplementedError(
            "Reading the BlueskyRun itself is not supported. Instead read one "
            "its entries, representing individual Event Streams.")

    def to_dask(self):
        raise NotImplementedError(
            "Reading the BlueskyRun itself is not supported. Instead read one "
            "its entries, representing individual Event Streams.")

    def _close(self):
        self.bag = None

    def canonical(self, *, fill, strict_order=True):
        """
        Yields documents from this Run in chronological order.

        Parameters
        ----------
        fill: {'yes', 'no'}
            If fill is 'yes', any external data referenced by Event documents
            will be filled in (e.g. images as numpy arrays). This is typically
            the desired option for *using* the data.
            If fill is 'no', the Event documents will contain foreign keys as
            placeholders for the data. This option is useful for exporting
            copies of the documents.
        strict_order : bool, optional
            documents are strictly yielded in ascending time order.
        """
        # Special case for 'delayed' since it *is* supported in the local mode
        # of usage.
        if fill == 'delayed':
            raise NotImplementedError(
                "Delayed access is not yet supported via the client--server "
                "usage.")
        FILL_OPTIONS = {'yes', 'no'}
        if fill not in FILL_OPTIONS:
            raise ValueError(f"Invalid fill option: {fill}, fill must be: {FILL_OPTIONS}")

        def stream_gen(entry):
            for i in itertools.count():
                partition = entry().read_partition({'index': i, 'fill': fill,
                                                  'partition_size': 'auto'})
                if not partition:
                    break
                yield from partition

        streams = [stream_gen(entry) for entry in self._entries.values()]

        yield ('start', self.metadata['start'])
        yield from interlace(*streams, strict_order=strict_order)
        yield ('stop', self.metadata['stop'])

    def read_canonical(self):
        warnings.warn(
            "The method read_canonical has been renamed canonical. This alias "
            "may be removed in a future release.")
        yield from self.canonical(fill='yes')

    def __repr__(self):
        self._load()
        try:
            start = self.metadata['start']
            stop = self.metadata['stop']
            out = (f"BlueskyRun\n"
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


class BlueskyRun(intake.catalog.Catalog):
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
    get_event_pages : callable
        Expected signature ``get_event_pages(descriptor_uid) -> generator``
        where ``generator`` yields Event documents
    get_event_count : callable
        Expected signature ``get_event_count(descriptor_uid) -> int``
    get_resource : callable
        Expected signature ``get_resource(resource_uid) -> Resource``
    get_resources: callable
        Expected signature ``get_resources() -> Resources``
    lookup_resource_for_datum : callable
        Expected signature ``lookup_resource_for_datum(datum_id) -> resource_uid``
    get_datum_pages : callable
        Expected signature ``get_datum_pages(resource_uid) -> generator``
        where ``generator`` yields Datum documents
    **kwargs :
        Additional keyword arguments are passed through to the base class,
        Catalog.
    handler_registry : dict, optional
        This is passed to the Filler or whatever class is given in the
        filler_class parametr below.

        Maps each 'spec' (a string identifying a given type or external
        resource) to a handler class.

        A 'handler class' may be any callable with the signature::

            handler_class(resource_path, root, **resource_kwargs)

        It is expected to return an object, a 'handler instance', which is also
        callable and has the following signature::

            handler_instance(**datum_kwargs)

        As the names 'handler class' and 'handler instance' suggest, this is
        typically implemented using a class that implements ``__init__`` and
        ``__call__``, with the respective signatures. But in general it may be
        any callable-that-returns-a-callable.
    root_map: dict, optional
        This is passed to Filler or whatever class is given in the filler_class
        parameter below.

        str -> str mapping to account for temporarily moved/copied/remounted
        files.  Any resources which have a ``root`` in ``root_map`` will be
        loaded using the mapped ``root``.
    filler_class: type
        This is Filler by default. It can be a Filler subclass,
        ``functools.partial(Filler, ...)``, or any class that provides the same
        methods as ``DocumentRouter``.
    """
    container = 'bluesky-run'
    version = '0.0.1'
    partition_access = True
    PARTITION_SIZE = 100

    def __init__(self,
                 get_run_start,
                 get_run_stop,
                 get_event_descriptors,
                 get_event_pages,
                 get_event_count,
                 get_resource,
                 get_resources,
                 lookup_resource_for_datum,
                 get_datum_pages,
                 get_filler,
                 entry,
                 **kwargs):
        # All **kwargs are passed up to base class. TODO: spell them out
        # explicitly.
        self.urlpath = ''  # TODO Not sure why I had to add this.

        self._get_run_start = get_run_start
        self._get_run_stop = get_run_stop
        self._get_event_descriptors = get_event_descriptors
        self._get_event_pages = get_event_pages
        self._get_event_count = get_event_count
        self._get_resource = get_resource
        self._get_resources = get_resources
        self._lookup_resource_for_datum = lookup_resource_for_datum
        self._get_datum_pages = get_datum_pages
        self.fillers = {}
        self.fillers['yes'] = get_filler(coerce='force_numpy')
        self.fillers['no'] = event_model.NoFiller(
            self.fillers['yes'].handler_registry, inplace=True)
        self.fillers['delayed'] = get_filler(coerce='delayed')
        self._entry = entry
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
        self._run_start_doc = self._get_run_start()
        self._run_stop_doc = self._get_run_stop()
        self._descriptors = self._get_event_descriptors()
        self._resources = self._get_resources() or []
        self.metadata.update({'start': self._run_start_doc})
        self.metadata.update({'stop': self._run_stop_doc})

        count = 1
        descriptor_uids = [doc['uid'] for doc in self._descriptors]
        count += len(descriptor_uids)
        for doc in self._descriptors:
            count += self._get_event_count(doc['uid'])
        count += (self._run_stop_doc is not None)

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
                get_event_pages=self._get_event_pages,
                get_event_count=self._get_event_count,
                get_resource=self._get_resource,
                lookup_resource_for_datum=self._lookup_resource_for_datum,
                get_datum_pages=self._get_datum_pages,
                fillers=self.fillers,
                metadata={'descriptors': descriptors,
                          'resources': self._resources})
            self._entries[stream_name] = intake.catalog.local.LocalCatalogEntry(
                name=stream_name,
                description={},  # TODO
                driver='databroker.core.BlueskyEventStream',
                direct_access='forbid',
                args=args,
                cache=None,  # What does this do?
                metadata={'descriptors': descriptors,
                          'resources': self._resources},
                catalog_dir=None,
                getenv=True,
                getshell=True,
                catalog=self)

    def get(self, *args, **kwargs):
        """
        Return self or, if args are provided, some new instance of type(self).

        This is here so that the user does not have to remember whether a given
        variable is a BlueskyRun or an *Entry* with a Bluesky Run. In either
        case, ``obj.get()`` will return a BlueskyRun.
        """
        return self._entry.get(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        """
        Return self or, if args are provided, some new instance of type(self).

        This is here so that the user does not have to remember whether a given
        variable is a BlueskyRun or an *Entry* with a Bluesky Run. In either
        case, ``obj()`` will return a BlueskyRun.
        """
        return self.get(*args, **kwargs)

    def __getattr__(self, key):
        try:
            # Let the base classes try to handle it first. This will handle,
            # for example, accessing subcatalogs using dot-access.
            return super().__getattr__(key)
        except AttributeError:
            # The user might be trying to access an Entry method. Try that
            # before giving up.
            return getattr(self._entry, key)

    def canonical(self, *, fill, strict_order=True):
        """
        Yields documents from this Run in chronological order.

        Parameters
        ----------
        fill: {'yes', 'no'}
            If fill is 'yes', any external data referenced by Event documents
            will be filled in (e.g. images as numpy arrays). This is typically
            the desired option for accessing small data.
            If fill is 'delayed', external data will be filled in as dask
            arrays, meaning that the I/O can be deferred until the data is
            actually needed.
            If fill is 'no', the Event documents will contain foreign keys as
            placeholders for the data. This option is useful for exporting
            copies of the documents.
        strict_order : bool, optional
            Documents are strictly yielded in ascending time order. This
            defaults to True.
        """
        FILL_OPTIONS = {'yes', 'no', 'delayed'}
        if fill not in FILL_OPTIONS:
            raise ValueError(f"Invalid fill option: {fill}, fill must be: {FILL_OPTIONS}")

        def stream_gen(entry):
            for i in itertools.count():
                partition = entry.read_partition({'index': i, 'fill': fill,
                                                  'partition_size': 'auto'})
                if not partition:
                    break
                yield from partition

        streams = [stream_gen(entry) for entry in self._entries.values()]

        yield ('start', self.metadata['start'])
        yield from interlace(*streams, strict_order=strict_order)
        yield ('stop', self.metadata['stop'])

    def read_canonical(self):
        warnings.warn(
            "The method read_canonical has been renamed canonical. This alias "
            "may be removed in a future release.")
        yield from self.canonical(fill='yes')

    def get_file_list(self, resource):
        """
        Fetch filepaths of external files associated with this Run.

        This method is not defined on RemoteBlueskyRun because the filepaths
        may not be meaningful on a remote machine.

        This method should be considered experimental. It may be changed or
        removed in a future release.
        """
        files = []
        # TODO Once event_model.Filler has a get_handler method, use that.
        try:
            handler_class = self.fillers['yes'].handler_registry[resource['spec']]
        except KeyError as err:
            raise event_model.UndefinedAssetSpecification(
                f"Resource document with uid {resource['uid']} "
                f"refers to spec {resource['spec']!r} which is "
                f"not defined in the Filler's "
                f"handler registry.") from err
        # Apply root_map.
        resource_path = resource['resource_path']
        root = resource.get('root', '')
        root = self.fillers['yes'].root_map.get(root, root)
        if root:
            resource_path = os.path.join(root, resource_path)

        handler = handler_class(resource_path,
                                **resource['resource_kwargs'])
        def datum_kwarg_gen():
            for page in self._get_datum_pages(resource['uid']):
                for datum in event_model.unpack_datum_page(page):
                    yield datum['datum_kwargs']

        files.extend(handler.get_file_list(datum_kwarg_gen()))
        return files

    def read(self):
        raise NotImplementedError(
            "Reading the BlueskyRun itself is not supported. Instead read one "
            "its entries, representing individual Event Streams. You can see "
            "the entries using list(YOUR_VARIABLE_HERE). Tab completion may "
            "also help, if available.")

    def to_dask(self):
        raise NotImplementedError(
            "Reading the BlueskyRun itself is not supported. Instead read one "
            "its entries, representing individual Event Streams. You can see "
            "the entries using list(YOUR_VARIABLE_HERE). Tab completion may "
            "also help, if available.")


class BlueskyEventStream(DataSourceMixin):
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
    get_event_pages : callable
        Expected signature ``get_event_pages(descriptor_uid) -> generator``
        where ``generator`` yields event_page documents
    get_event_count : callable
        Expected signature ``get_event_count(descriptor_uid) -> int``
    get_resource : callable
        Expected signature ``get_resource(resource_uid) -> Resource``
    lookup_resource_for_datum : callable
        Expected signature ``lookup_resource_for_datum(datum_id) -> resource_uid``
    get_datum_pages : callable
        Expected signature ``get_datum_pages(resource_uid) -> generator``
        where ``generator`` yields datum_page documents
    fillers : dict of Fillers
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
    container = 'bluesky-event-stream'
    version = '0.0.1'
    partition_access = True

    def __init__(self,
                 get_run_start,
                 stream_name,
                 get_run_stop,
                 get_event_descriptors,
                 get_event_pages,
                 get_event_count,
                 get_resource,
                 lookup_resource_for_datum,
                 get_datum_pages,
                 fillers,
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
        self._get_event_pages = get_event_pages
        self._get_event_count = get_event_count
        self._get_resource = get_resource
        self._lookup_resource_for_datum = lookup_resource_for_datum
        self._get_datum_pages = get_datum_pages
        self.fillers = fillers
        self.urlpath = ''  # TODO Not sure why I had to add this.
        self._ds = None  # set by _open_dataset below
        self.include = include
        self.exclude = exclude

        super().__init__(metadata=metadata, **kwargs)

        self._run_stop_doc = self._get_run_stop()
        self._run_start_doc = self._get_run_start()
        self._descriptors =  [descriptor for descriptor in
                              metadata.get('descriptors', [])
                              if descriptor.get('name') == self._stream_name]
        # Should figure out a way so that self._resources doesn't have to be
        # all of the Run's resources.
        self._resources = metadata.get('resources', [])
        self.metadata.update({'start': self._run_start_doc})
        self.metadata.update({'stop': self._run_stop_doc})
        self._partitions = None

    def __repr__(self):
        try:
            out = (f"<Intake catalog: Stream {self._stream_name!r} "
                   f"from Run {self._get_run_start()['uid'][:8]}...>")
        except Exception as exc:
            out = f"<Intake catalog: Stream *REPR_RENDERING_FAILURE* {exc!r}>"
        return out

    def _open_dataset(self):
        self._ds = documents_to_xarray(
            start_doc=self._run_start_doc,
            stop_doc=self._run_stop_doc,
            descriptor_docs=self._descriptors,
            get_event_pages=self._get_event_pages,
            filler=self.fillers['delayed'],
            get_resource=self._get_resource,
            lookup_resource_for_datum=self._lookup_resource_for_datum,
            get_datum_pages=self._get_datum_pages,
            include=self.include,
            exclude=self.exclude)

    def read(self):
        """
        Return data from this Event Stream as an xarray.Dataset.

        This loads all of the data into memory. For delayed ("lazy"), chunked
        access to the data, see :meth:`to_dask`.
        """
        # Implemented just so we can put in a docstring
        return super().read()

    def to_dask(self):
        """
        Return data from this Event Stream as an xarray.Dataset backed by dask.
        """
        # Implemented just so we can put in a docstring
        return super().to_dask()

    def _load_partitions(self, partition_size):
        datum_gens = [self._get_datum_pages(resource['uid'])
                      for resource in self._resources]
        event_gens = [list(self._get_event_pages(descriptor['uid']))
                      for descriptor in self._descriptors]
        self._partitions = list(
            unfilled_partitions(self._run_start_doc, self._descriptors,
                                self._resources, self._run_stop_doc,
                                datum_gens, event_gens, partition_size))
        self.npartitions = len(self._partitions)

    def read_partition(self, partition):
        """Fetch one chunk of documents.
        """
        if isinstance(partition, (tuple, list)):
            return super().read_partition(partition)

        # Unpack partition
        i = partition['index']

        if isinstance(partition['fill'], str):
            filler = self.fillers[partition['fill']]
        else:
            filler = partition['fill']

        # Partition size is the number of pages in the partition.
        if partition['partition_size'] == 'auto':
            partition_size = 5
        elif isinstance(partition['partition_size'], int):
            partition_size = partition['partition_size']
        else:
            raise ValueError(f"Invalid partition_size {partition['partition_size']}")

        if self._partitions is None:
            self._load_partitions(partition_size)
        try:
            try:
                return [filler(name, doc) for name, doc in self._partitions[i]]
            except event_model.UnresolvableForeignKeyError as err:
                # Slow path: This error should only happen if there is an old style
                # resource document that doesn't have a run_start key.
                self._partitions[i:i] = self._missing_datum(err.key, self.PARTITION_SIZE)
                return [filler(name, doc) for name, doc in self._partitions[i]]
        except IndexError as e:
            return []

    def _missing_datum(self, datum_id, partition_size):

        # Get the resource from the datum_id.
        if '/' in datum_id:
            resource_uid, _ = datum_id.split('/', 1)
        else:
            resource_uid = self._lookup_resource_for_datum(datum_id)
        resource = self._get_resource(uid=resource_uid)

        # Use rechunk datum pages to make them into pages of size "partition_size"
        # and yield one page per partition.  Rechunk might be slow.
        datum_gen = self._get_datum_pages(resource['uid'])
        partitions = [[('datum_page', datum_page)] for datum_page in
                      event_model.rechunk_datum_pages(datum_gen, partition_size)]

        # Check that the datum_id from the exception has been added.
        def check():
            for partition in partitions:
                for name, datum_page in partition:
                    if datum_id in datum_page['datum_id']:
                        return True
            return False

        if not check():
            raise

        # Add the resource to the begining of the first partition.
        partitions[0] = [('resource', resource)] + partitions[0]
        self.npartitions += len(partitions)
        return partitions

    def _get_partition(self, partition):
        return intake.container.base.get_partition(self.url, self.http_args,
                                             self._source_id, self.container,
                                             partition)

class RemoteBlueskyEventStream(RemoteXarray):
    # Because of the container_map, when working in remote mode, when accessing
    # a BlueskyRun or BlueskyEventStream, you will get a RemoteBlueskyRun or a
    # RemoteBlueskyEventStream on the client side. Canonical of the RemoteBlueskyRun,
    # calls read_partition of the RemoteBlueskyEventStream, where there
    # partition argument is a dict. The inherited read_partition method only
    # accepts an integer for the partition argument, so read_partition needs to
    # be overridden.
    def read_partition(self, partition):
        self._load_metadata()
        return self._get_partition(partition)


class DocumentCache(event_model.DocumentRouter):
    def __init__(self):
        self.descriptors = {}
        self.resources = {}
        self.event_pages = collections.defaultdict(list)
        self.datum_pages_by_resource = collections.defaultdict(list)
        self.resource_uid_by_datum_id = {}
        self.start_doc = None
        self.stop_doc = None

    def start(self, doc):
        self.start_doc = doc

    def stop(self, doc):
        self.stop_doc = doc

    def event_page(self, doc):
        self.event_pages[doc['descriptor']].append(doc)

    def datum_page(self, doc):
        self.datum_pages_by_resource[doc['resource']].append(doc)
        for datum_id in doc['datum_id']:
            self.resource_uid_by_datum_id[datum_id] = doc['resource']

    def descriptor(self, doc):
        self.descriptors[doc['uid']] = doc

    def resource(self, doc):
        self.resources[doc['uid']] = doc


class BlueskyRunFromGenerator(BlueskyRun):

    def __init__(self, gen_func, gen_args, gen_kwargs, get_filler, **kwargs):

        document_cache = DocumentCache()

        for item in gen_func(*gen_args, **gen_kwargs):
            document_cache(*item)

        assert document_cache.start_doc is not None

        def get_run_start():
            return document_cache.start_doc

        def get_run_stop():
            return document_cache.stop_doc

        def get_event_descriptors():
            return document_cache.descriptors.values()

        def get_event_pages(descriptor_uid, skip=0, limit=None):
            if skip != 0 and limit is not None:
                raise NotImplementedError
            return document_cache.event_pages[descriptor_uid]

        def get_event_count(descriptor_uid):
            return sum(len(page['seq_num'])
                       for page in (document_cache.event_pages[descriptor_uid]))

        def get_resource(uid):
            return document_cache.resources[uid]

        def get_resources():
            return list(document_cache.resources.values())

        def lookup_resource_for_datum(datum_id):
            return document_cache.resource_uid_by_datum_id[datum_id]

        def get_datum_pages(resource_uid, skip=0, limit=None):
            if skip != 0 and limit is not None:
                raise NotImplementedError
            return document_cache.datum_pages_by_resource[resource_uid]

        super().__init__(
            get_run_start=get_run_start,
            get_run_stop=get_run_stop,
            get_event_descriptors=get_event_descriptors,
            get_event_pages=get_event_pages,
            get_event_count=get_event_count,
            get_resource=get_resource,
            get_resources=get_resources,
            lookup_resource_for_datum=lookup_resource_for_datum,
            get_datum_pages=get_datum_pages,
            get_filler=get_filler,
            **kwargs)


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
    for k in keys:
        out[k] = dask.array.stack(out[k])
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


def discover_handlers(entrypoint_group_name='databroker.handlers',
                      skip_failures=True):
    """
    Discover handlers via entrypoints.

    Parameters
    ----------
    entrypoint_group_name: str
        Default is 'databroker.handlers', the "official" databroker entrypoint
        for handlers.
    skip_failures: boolean
        True by default. Errors loading a handler class are converted to
        warnings if this is True.

    Returns
    -------
    handler_registry: dict
        A suitable default handler registry
    """
    group = entrypoints.get_group_named(entrypoint_group_name)
    group_all = entrypoints.get_group_all(entrypoint_group_name)
    if len(group_all) != len(group):
        # There are some name collisions. Let's go digging for them.
        for name, matches in itertools.groupby(group_all, lambda ep: ep.name):
            matches = list(matches)
            if len(matches) != 1:
                winner = group[name]
                warnings.warn(
                    f"There are {len(matches)} entrypoints for the "
                    f"databroker handler spec {name!r}. "
                    f"They are {matches}. The match {winner} has won the race.")
    handler_registry = {}
    for name, entrypoint in group.items():
        try:
            handler_class = entrypoint.load()
        except Exception as exc:
            if skip_failures:
                warnings.warn("Skipping {entrypoint!r} which failed to load. "
                              "Exception: {exc!r}")
                continue
            else:
                raise
        handler_registry[name] = entrypoint.load()

    return handler_registry


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


# This determines the type of the class that you get on the
# client side.
intake.container.container_map['bluesky-run'] = RemoteBlueskyRun
intake.container.container_map['bluesky-event-stream'] = RemoteBlueskyEventStream


def concat_dataarray_pages(dataarray_pages):
    """
    Combines a iterable of dataarray_pages to a single dataarray_page.

    Parameters
    ----------
    dataarray_pages: Iterabile
        An iterable of event_pages with xarray.dataArrays in the data,
        timestamp, and filled fields.
    Returns
    ------
    event_page : dict
        A single event_pages with xarray.dataArrays in the data,
        timestamp, and filled fields.
    """
    pages = list(dataarray_pages)
    if len(pages) == 1:
        return pages[0]

    array_keys = ['seq_num', 'time', 'uid']
    data_keys = dataarray_pages[0]['data'].keys()

    return {'descriptor': pages[0]['descriptor'],
            **{key: list(itertools.chain.from_iterable(
                    [page[key] for page in pages])) for key in array_keys},
            'data': {key: xarray.concat([page['data'][key] for page in pages],
                                        dim='concat_dim')
                     for key in data_keys},
            'timestamps': {key: xarray.concat([page['timestamps'][key]
                                              for page in pages], dim='concat_dim')
                           for key in data_keys},
            'filled': {key: xarray.concat([page['filled'][key]
                                          for page in pages], dim='concat_dim')
                       for key in data_keys}}


def event_page_to_dataarray_page(event_page, dims=None, coords=None):
    """
    Converts the event_page's data, timestamps, and filled to xarray.DataArray.

    Parameters
    ----------
    event_page: dict
        A EventPage document
    dims: tuple
        Tuple of dimension names associated with the array
    coords: dict-like
        Dictionary-like container of coordinate arrays
    Returns
    ------
    event_page : dict
        An event_pages with xarray.dataArrays in the data,
        timestamp, and filled fields.
    """
    if coords is None:
        coords = {'time': event_page['time']}
    if dims is None:
        dims = ('time',)

    array_keys = ['seq_num', 'time', 'uid']
    data_keys = event_page['data'].keys()

    return {'descriptor': event_page['descriptor'],
            **{key: event_page[key] for key in array_keys},
            'data': {key: xarray.DataArray(
                            event_page['data'][key], dims=dims, coords=coords, name=key)
                     for key in data_keys},
            'timestamps': {key: xarray.DataArray(
                            event_page['timestamps'][key], dims=dims, coords=coords, name=key)
                           for key in data_keys},
            'filled': {key: xarray.DataArray(
                            event_page['filled'][key], dims=dims, coords=coords, name=key)
                       for key in data_keys}}


def dataarray_page_to_dataset_page(dataarray_page):

    """
    Converts the dataarray_page's data, timestamps, and filled to xarray.DataSet.

    Parameters
    ----------
    dataarray_page: dict
    Returns
    ------
    dataset_page : dict
    """
    array_keys = ['seq_num', 'time', 'uid']

    return {'descriptor': dataarray_page['descriptor'],
            **{key: dataarray_page[key] for key in array_keys},
            'data': xarray.merge(dataarray_page['data'].values()),
            'timestamps': xarray.merge(dataarray_page['timestamps'].values()),
            'filled': xarray.merge(dataarray_page['filled'].values())}


def coerce_dask(handler_class, filler_state):
    # If the handler has its own delayed logic, defer to that.
    if hasattr(handler_class, 'return_type'):
        if handler_class.return_type['delayed']:
            return handler_class

    # Otherwise, provide best-effort dask support by wrapping each datum
    # payload in dask.array.from_delayed. This means that each datum will be
    # one dask task---it cannot be rechunked into multiple tasks---but that
    # may be sufficient for many handlers.
    class Subclass(handler_class):

        def __call__(self, *args, **kwargs):
            descriptor = filler_state.descriptor
            key = filler_state.key
            shape = extract_shape(descriptor, key)
            dtype = extract_dtype(descriptor, key)
            load_chunk = dask.delayed(super().__call__)(*args, **kwargs)
            return dask.array.from_delayed(load_chunk, shape=shape, dtype=dtype)

    return Subclass


# This adds a 'delayed' option to event_model.Filler's `coerce` parameter.
# By adding it via plugin, we avoid adding a dask.array dependency to
# event-model and we keep the fiddly hacks into extract_shape here in
# databroker, a faster-moving and less fundamental library than event-model.
event_model.register_coersion('delayed', coerce_dask)


def extract_shape(descriptor, key):
    """
    Work around bug in https://github.com/bluesky/ophyd/pull/746
    """
    # Ideally this code would just be
    # descriptor['data_keys'][key]['shape']
    # but we have to do some heuristics to make up for errors in the reporting.

    # Broken ophyd reports (x, y, 0). We want (num_images, y, x).
    data_key = descriptor['data_keys'][key]
    if len(data_key['shape']) == 3 and data_key['shape'][-1] == 0:
        object_keys = descriptor.get('object_keys', {})
        for object_name, data_keys in object_keys.items():
            if key in data_keys:
                break
        else:
            raise RuntimeError(f"Could not figure out shape of {key}")
        for k, v in descriptor['configuration'][object_name]['data'].items():
            if k.endswith('num_images'):
                num_images = v
                break
        else:
            num_images = -1
        x, y, _ = data_key['shape']
        shape = (num_images, y, x)
    else:
        shape = descriptor['data_keys'][key]['shape']
    return shape


def extract_dtype(descriptor, key):
    """
    Work around the fact that we currently report jsonschema data types.
    """
    reported = descriptor['data_keys'][key]['dtype']
    if reported == 'array':
        return float  # guess!
    else:
        return reported


# This comes from the old databroker.core from before intake-bluesky was merged
# in. It apparently was useful for back-compat at some point.
from databroker._core import Header
