import collections
import copy
import entrypoints
import event_model
from datetime import datetime
import dask
import dask.bag
import functools
import heapq
import importlib
import itertools
import logging
import cachetools
from dask.base import tokenize
import intake.catalog.base
import intake.catalog.local
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
from .utils import LazyMap
from bluesky_live.conversion import documents_to_xarray, documents_to_xarray_config
from collections import deque, OrderedDict
from dask.base import normalize_token

try:
    from intake.catalog.remote import RemoteCatalog as intake_RemoteCatalog
except ImportError:
    from intake.catalog.base import RemoteCatalog as intake_RemoteCatalog

logger = logging.getLogger(__name__)


class NotMutable(Exception):
    ...


class Document(dict):
    """
    Document is an immutable dict subclass.

    It is immutable to help consumer code avoid accidentally corrupting data
    that another part of the cosumer code was expected to use unchanged.

    Subclasses of Document must define __dask_tokenize__. The tokenization
    schemes typically uniquely identify the document based on only a subset of
    its contents, and mutating the contents can thereby create situations where
    two unequal objects have colliding tokens. Immutability helps guard against
    this too.

    Note that Documents are not *recursively* immutable. Just as it is possible
    create a tuple (immutable) of lists (mutable) and mutate the lists, it is
    possible to mutate the internal contents of a Document, but this should not
    be done. It is safer to use the to_dict() method to create a mutable deep
    copy.

    This is implemented as a dict subclass in order to satisfy certain
    consumers that expect an object that satisfies isinstance(obj, dict).
    This implementation detail may change in the future.
    """

    __slots__ = ("__not_a_real_dict",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # This lets pickle recognize that this is not a literal dict and that
        # it should respect its custom __setstate__.
        self.__not_a_real_dict = True

    def __repr__(self):
        # same as dict, but wrapped in the class name so the eval round-trips
        return f"{self.__class__.__name__}({dict(self)})"

    def _repr_pretty_(self, p, cycle):
        """
        A multi-line but eval-able text repr with readable indentation

        This hooks into IPython/Jupyter's display mechanism
        This is *not* invoked by print() or repr(), but it is invoked by
        IPython.display.display() which is called in this common scenario::

            In [1]: doc = Document(...)
            In [2]: doc
            <pretty representation will show here>
        """
        # Note: IPython's pretty-prettying mechanism is custom and complex.
        # The `text` method used below is a direct and blunt way to engage it
        # and seems widely used in the IPython code base. There are other
        # specific mechanisms for displaying collections like dicts, but they
        # can *truncate* which I think we want to avoid and they would require
        # more investment to understand how to use.
        from pprint import pformat
        return p.text(f"{self.__class__.__name__}({pformat(dict(self))})")

    def __getstate__(self):
        return dict(self)

    def __setstate__(self, state):
        dict.update(self, state)
        self.__not_a_real_dict = True

    def __readonly(self, *args, **kwargs):
        raise NotMutable(
            "Documents are not mutable. Call the method to_dict() to make a "
            "fully independent and mutable deep copy."
        )

    def __setitem__(self, key, value):
        try:
            self.__not_a_real_dict
        except AttributeError:
            # This path is necessary to support un-pickling.
            return dict.__setitem__(self, key, value)
        else:
            self.__readonly()

    __delitem__ = __readonly
    pop = __readonly
    popitem = __readonly
    clear = __readonly
    setdefault = __readonly
    update = __readonly

    def to_dict(self):
        """
        Create a mutable deep copy.
        """
        # Convert to dict and then make a deep copy to ensure that if the user
        # mutates any internally nested dicts there is no spooky action at a
        # distance.
        return copy.deepcopy(dict(self))

    def __deepcopy__(self, memo):
        # Without this, copy.deepcopy(Document(...)) fails because deepcopy
        # creates a new, empty Document instance and then tries to add items to
        # it.
        return self.__class__({k: copy.deepcopy(v, memo) for k, v in self.items()})

    def __dask_tokenize__(self):
        raise NotImplementedError


# We must use dask's registration mechanism to tell it to treat Document
# specially. Dask's tokenization dispatch mechanism discovers that Docuemnt is
# a dict subclass and treats it as a dict, ignoring its __dask_tokenize__
# method. To force it to respect our cutsom tokenization, we must explicitly
# register it.


@normalize_token.register(Document)
def tokenize_document(instance):
    return instance.__dask_tokenize__()


class Start(Document):
    def __dask_tokenize__(self):
        return ('start', self['uid'])


class Stop(Document):
    def __dask_tokenize__(self):
        return ('stop', self['uid'])


class Resource(Document):
    def __dask_tokenize__(self):
        return ('resource', self['uid'])


class Descriptor(Document):
    def __dask_tokenize__(self):
        return ('descriptor', self['uid'])


class Event(Document):
    def __dask_tokenize__(self):
        return ('event', self['uid'])


class EventPage(Document):
    def __dask_tokenize__(self):
        return ('event_page', self['uid'])


class Datum(Document):
    def __dask_tokenize__(self):
        return ('datum', self['datum_id'])


class DatumPage(Document):
    def __dask_tokenize__(self):
        return ('datum_page', self['uid'])


class PartitionIndexError(IndexError):
    ...


class Entry(intake.catalog.local.LocalCatalogEntry):

    @property
    def _pmode(self):
        return 'never'

    @_pmode.setter
    def _pmode(self, val):
        ...

    def __init__(self, **kwargs):
        # This might never come up, but just to be safe....
        if 'entry' in kwargs['args']:
            raise TypeError("The args cannot contain 'entry'. It is reserved.")
        super().__init__(**kwargs)
        # This cache holds datasources, the result of calling super().get(...)
        # with potentially different arguments.
        self.__cache = self._make_cache()
        self.entry = self
        logger.debug("Created Entry named %r", self.name)

    def _repr_pretty_(self, p, cycle):
        return self.get()._repr_pretty_(p, cycle)

    def _repr_mimebundle_(self, include=None, exclude=None):
        return self.get()._repr_mimebundle_(include=include, exclude=exclude)

    @property
    def catalog(self):
        return self._catalog

    def _make_cache(self):
        return cachetools.LRUCache(10)

    def _create_open_args(self, user_parameters):
        plugin, open_args = super()._create_open_args(user_parameters)
        # Inject self into arguments passed to instanitate the driver. This
        # enables the driver instance to know which Entry created it.
        open_args['entry'] = self
        return plugin, open_args

    def cache_clear(self):
        self.__cache.clear()

    def get(self, **kwargs):
        token = tokenize(OrderedDict(kwargs))
        try:
            datasource = self.__cache[token]
            logger.debug(
                "Entry cache found %s named %r",
                datasource.__class__.__name__,
                datasource.name)
        except KeyError:
            datasource = super().get(**kwargs)
            self.__cache[token] = datasource
        return datasource

    # def __dask_tokenize__(self):
    #     print('bob')
    #     metadata = self.describe()['metadata']
    #     return ('Entry', metadata['start']['uid'])


class StreamEntry(Entry):
    """
    This is a temporary fix that is being proposed to include in intake.
    """
    def _make_cache(self):
        return dict()

    # def __dask_tokenize__(self):
    #     print('bill')
    #     metadata = self.describe()['metadata']
    #     print(self.describe())
    #     return ('Stream', metadata['start']['uid'], self.name)


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


def retry(function):
    """
    Decorator that retries a Catalog function once.

    Parameters
    ----------
    function: function

    Returns
    -------
    new_function: function
    """

    @functools.wraps(function)
    def new_function(self, *args, **kwargs):
        try:
            return function(self, *args, **kwargs)
        except Exception:
            self.force_reload()
            return function(self, *args, **kwargs)

    return new_function


def _flatten_event_page_gen(gen):
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


def _interlace_event_pages(*gens):
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

    def safe_next(index):
        try:
            val = next(iters[index])
        except StopIteration:
            return
        heapq.heappush(heap, (val['time'][0], val['uid'][0], index, val))

    for i in range(len(iters)):
        safe_next(i)

    while heap:
        _, _, index, val = heapq.heappop(heap)
        yield val
        safe_next(index)


def _interlace_event_page_chunks(*gens, chunk_size):
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
    yield from _interlace_event_pages(*iters)


def _interlace(*gens, strict_order=True):
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
    # appending documents that are not events/event_pages to the fifo.
    def get_next(index):
        while True:
            try:
                name, doc = next(iters[index])
            except StopIteration:
                return
            if name == 'event':
                heapq.heappush(heap, (doc['time'], doc['uid'], index, (name, doc)))
                return
            elif name == 'event_page':
                if strict_order:
                    for event in event_model.unpack_event_page(doc):
                        event_page = event_model.pack_event_page(event)
                        heapq.heappush(heap, (event_page['time'][0], event_page['uid'][0],
                                              index, ('event_page', event_page)))
                    return
                else:
                    heapq.heappush(heap, (doc['time'][0], doc['uid'][0], index, (name, doc)))
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
        _, _, index, doc = heapq.heappop(heap)
        yield doc
        get_next(index)

    # Yield any remaining items in the fifo queue.
    while fifo:
        yield fifo.popleft()


def _unfilled_partitions(start, descriptors, resources, stop, datum_gens,
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
        partition = [('datum_page', datum_page) for datum_page in
                     event_model.rechunk_datum_pages(datum_gen, partition_size)]
        if partition:
            yield partition

    # Rechunk the event pages and interlace them in timestamp order, then pack
    # them into a partition.
    count = 0
    partition = []
    for event_page in _interlace_event_pages(*event_gens):
        partition.append(('event_page', event_page))
        count += 1
        if count == partition_size:
            yield partition
            count = 0
            partition = []

    # Add the stop document onto the last partition.
    partition.append(('stop', stop))
    yield partition


def _fill(filler,
          event,
          lookup_resource_for_datum,
          get_resource,
          get_datum_pages,
          last_datum_id=None):
    try:
        _, filled_event = filler("event", event)
        return filled_event
    except event_model.UnresolvableForeignKeyError as err:
        datum_id = err.key
        if datum_id == last_datum_id:
            # We tried to fetch this Datum on the last trip
            # trip through this method, and apparently it did not
            # work. We are in an infinite loop. Bail!
            raise

        # try to fast-path looking up the resource uid if this works
        # it saves us a a database hit (to get the datum document)
        if "/" in datum_id:
            resource_uid, _ = datum_id.split("/", 1)
        # otherwise do it the standard way
        else:
            resource_uid = lookup_resource_for_datum(datum_id)

        # but, it might be the case that the key just happens to have
        # a '/' in it and it does not have any semantic meaning so we
        # optimistically try
        try:
            resource = get_resource(uid=resource_uid)
        # and then fall back to the standard way to be safe
        except ValueError:
            resource = get_resource(lookup_resource_for_datum(datum_id))

        filler("resource", resource)
        # Pre-fetch all datum for this resource.
        for datum_page in get_datum_pages(resource_uid=resource_uid):
            filler("datum_page", datum_page)
        # TODO -- When to clear the datum cache in filler?

        # Re-enter and try again now that the Filler has consumed the
        # missing Datum. There might be another missing Datum in this same
        # Event document (hence this re-entrant structure) or might be good
        # to go.
        return _fill(
            filler,
            event,
            lookup_resource_for_datum,
            get_resource,
            get_datum_pages,
            last_datum_id=datum_id,
        )


def _documents(*, start, stop, entries, fill, strict_order=True):
    """
    Yields documents from this Run in chronological order.

    Parameters
    ----------
    start_doc : dict
        RunStart Document
    stop_doc : dict
        RunStop Document
    entries : dict
        A dict of the BlueskyRun's entries.
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
    history = set()

    FILL_OPTIONS = {'yes', 'no', 'delayed'}
    if fill not in FILL_OPTIONS:
        raise ValueError(f"Invalid fill option: {fill}, fill must be: {FILL_OPTIONS}")

    def stream_gen(entry):
        for i in itertools.count():
            partition = entry().read_partition({'index': i, 'fill': fill,
                                                'partition_size': 'auto'})
            if not partition:
                break
            yield from partition

    streams = [stream_gen(entry) for entry in entries.values()]
    yield ('start', start)

    # This following code filters out duplicate documents.
    # This is needed because we dont know which EventStream, that resource
    # or datum documents belong to, so each stream has these documents.
    # Without this filter we would get multiple of the same resource and
    # datum documents.
    for name, doc in _interlace(*streams, strict_order=strict_order):

        if name == 'datum':
            if doc['datum_id'] not in history:
                yield (name, doc)
                history.add(doc['datum_id'])

        if name == 'datum_page':
            if tuple(doc['datum_id']) not in history:
                yield (name, doc)
                history.add(tuple(doc['datum_id']))

        elif name == 'resource':
            if doc['uid'] not in history:
                yield (name, doc)
                history.add(doc['uid'])

        else:
            yield (name, doc)
    if stop is not None:
        yield ('stop', stop)


class RemoteBlueskyRun(intake_RemoteCatalog):
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

    # opt-out of the persistence features of intake

    @property
    def has_been_persisted(self):
        return False

    @property
    def is_persisted(self):
        return False

    def get_persisted(self):
        raise KeyError("Does not support intake persistence")

    def persist(self, *args, **kwargs):
        raise NotImplementedError

    @property
    def pmode(self):
        return 'never'

    @pmode.setter
    def pmode(self, val):
        ...

    # def __dask_tokenize__(self):
    #     print('baz')
    #     return ('RemoteBlueskyRun', self.metadata['start']['uid'])

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
        # turn off any attempts at persistence
        self._pmode = "never"
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

    def documents(self, *, fill, strict_order=True):
        # Special case for 'delayed' since it *is* supported in the local mode
        # of usage.
        if fill == 'delayed':
            raise NotImplementedError(
                "Delayed access is not yet supported via the client--server "
                "usage.")

        yield from _documents(start=self.metadata['start'],
                              stop=self.metadata['stop'],
                              entries=self._entries,
                              fill=fill,
                              strict_order=strict_order)

    def read_canonical(self):
        warnings.warn(
            "The method read_canonical has been renamed documents. This alias "
            "may be removed in a future release.")
        yield from self.documents(fill='yes')

    def canonical(self, *, fill, strict_order=True):
        warnings.warn(
            "The method canonical has been renamed documents. This alias "
            "may be removed in a future release.")
        yield from self.documents(fill=fill, strict_order=strict_order)

    def __repr__(self):
        try:
            self._load()
            start = self.metadata['start']
            return f"<{self.__class__.__name__} uid={start['uid']!r}>"
        except Exception as exc:
            return f"<{self.__class__.__name__} *REPR RENDERING FAILURE* {exc!r}>"

    def _repr_pretty_(self, p, cycle):
        try:
            self._load()
            start = self.metadata['start']
            stop = self.metadata['stop'] or {}
            out = (f"BlueskyRun\n"
                   f"  uid={start['uid']!r}\n"
                   f"  exit_status={stop.get('exit_status')!r}\n"
                   f"  {_ft(start['time'])} -- {_ft(stop.get('time', '?'))}\n"
                   f"  Streams:\n")
            for stream_name in self:
                out += f"    * {stream_name}\n"
        except Exception as exc:
            out = f"<{self.__class__.__name__} *REPR_RENDERING_FAILURE* {exc!r}>"
        p.text(out)

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
    get_filler : callable
        Expected signature ``get_filler() -> event_model.Filler``
    transforms : Dict[str, Callable]
        A dict that maps any subset of the keys {start, stop, resource, descriptor}
        to a function that accepts a document of the corresponding type and
        returns it, potentially modified. This feature is for patching up
        erroneous metadata. It is intended for quick, temporary fixes that
        may later be applied permanently to the data at rest
        (e.g., via a database migration).
    **kwargs :
        Additional keyword arguments are passed through to the base class,
        Catalog.
    """
    # Work around
    # https://github.com/intake/intake/issues/545
    _container = None

    # opt-out of the persistence features of intake
    @property
    def has_been_persisted(self):
        return False

    @property
    def is_persisted(self):
        return False

    def get_persisted(self):
        raise KeyError("Does not support intake persistence")

    def persist(self, *args, **kwargs):
        raise NotImplementedError

    @property
    def pmode(self):
        return 'never'

    @pmode.setter
    def pmode(self, val):
        ...

    # def __dask_tokenize__(self):
    #     print('baz')
    #     return ('BlueksyRun', self.metadata['start']['uid'])

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
                 transforms,
                 **kwargs):
        # Set the name here, earlier than the base class does, so that the log
        # message in self._load has access to it.
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
        self._transforms = transforms
        self._run_stop_doc = None
        self.__entry = entry
        super().__init__(**{**kwargs, 'persist_mode': 'never'})
        # turn off any attempts at persistence
        self._pmode = "never"
        logger.debug(
            "Created %s named %r",
            self.__class__.__name__,
            entry.name)

    def describe(self):
        return self.__entry.describe()

    def __repr__(self):
        try:
            self._load()
            start = self.metadata['start']
            return f"<{self.__class__.__name__} uid={start['uid']!r}>"
        except Exception as exc:
            return f"<{self.__class__.__name__} *REPR RENDERING FAILURE* {exc!r}>"

    def _repr_pretty_(self, p, cycle):
        try:
            self._load()
            start = self.metadata['start']
            stop = self.metadata['stop'] or {}
            out = (f"BlueskyRun\n"
                   f"  uid={start['uid']!r}\n"
                   f"  exit_status={stop.get('exit_status')!r}\n"
                   f"  {_ft(start['time'])} -- {_ft(stop.get('time', '?'))}\n"
                   f"  Streams:\n")
            for stream_name in self:
                out += f"    * {stream_name}\n"
        except Exception as exc:
            out = f"<{self.__class__.__name__} *REPR_RENDERING_FAILURE* {exc!r}>"
        p.text(out)

    def _repr_mimebundle_(self, include=None, exclude=None):
        # TODO Make a nice 'text/html' repr here. For now just override
        # intake's which is unreadably verbose for us.
        return {}

    _ipython_display_ = None

    def _make_entries_container(self):
        return LazyMap()

    def _load(self):
        self._run_start_doc = Start(self._transforms['start'](self._get_run_start()))

        # get_run_stop() may return None if the document was never created due
        # to a critical failure or simply not yet emitted during a Run that is
        # still in progress. If it returns None, pass that through.
        if self._run_stop_doc is None:
            stop = self._get_run_stop()
            if stop is None:
                self._run_stop_doc = stop
            else:
                self._run_stop_doc = Stop(self._transforms['stop'](stop))

        self.metadata.update({'start': self._run_start_doc})
        self.metadata.update({'stop': self._run_stop_doc})

        # TODO Add driver API to allow us to fetch just the stream names not
        # all the descriptors. We don't need them until BlueskyEventStream.
        self._descriptors = [self._transforms['descriptor'](descriptor)
                             for descriptor in self._get_event_descriptors()]

        # Count the total number of documents in this run.
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
        stream_names = set(doc.get('name', 'primary') for doc in self._descriptors)
        new_stream_names = stream_names - set(self._entries)

        def wrapper(stream_name, metadata, args):
            return StreamEntry(name=stream_name,
                               description={},  # TODO
                               driver='databroker.core.BlueskyEventStream',
                               direct_access='forbid',
                               args=args,
                               cache=None,  # What does this do?
                               metadata=metadata,
                               catalog_dir=None,
                               getenv=True,
                               getshell=True,
                               catalog=self)

        # We employ OrderedDict in several places in this loop. The motivation
        # is to speed up dask tokenization. When dask tokenizes a plain dict,
        # it sorts the keys, and it turns out that this sort operation
        # dominates the call time, even for very small dicts. Using an
        # OrderedDict steers dask toward a different and faster tokenization.
        new_entries = {}
        for stream_name in new_stream_names:
            metadata = OrderedDict({'start': self.metadata['start'],
                                    'stop': self.metadata['stop']})
            args = OrderedDict(
                stream_name=stream_name,
                get_run_stop=self._get_run_stop,
                get_event_descriptors=self._get_event_descriptors,
                get_event_pages=self._get_event_pages,
                get_event_count=self._get_event_count,
                get_resource=self._get_resource,
                get_resources=self._get_resources,
                lookup_resource_for_datum=self._lookup_resource_for_datum,
                get_datum_pages=self._get_datum_pages,
                fillers=OrderedDict(self.fillers),
                transforms=OrderedDict(self._transforms),
                metadata=metadata)

            new_entries[stream_name] = functools.partial(wrapper, stream_name,
                                                         metadata, args)
        self._entries.add(new_entries)
        logger.debug(
            "Loaded %s named %r",
            self.__class__.__name__,
            self.__entry.name)

    def configure_new(self, **kwargs):
        """
        Return self or, if args are provided, some new instance of type(self).

        This is here so that the user does not have to remember whether a given
        variable is a BlueskyRun or an *Entry* with a Bluesky Run. In either
        case, ``obj()`` will return a BlueskyRun.
        """
        return self.__entry.get(**kwargs)

    get = __call__ = configure_new

    def documents(self, *, fill, strict_order=True):
        yield from _documents(start=self.metadata['start'],
                              stop=self.metadata['stop'],
                              entries=self._entries,
                              fill=fill,
                              strict_order=strict_order)

    def read_canonical(self):
        warnings.warn(
            "The method read_canonical has been renamed documents. This alias "
            "may be removed in a future release.")
        yield from self.documents(fill='yes')

    def canonical(self, *, fill, strict_order=True):
        warnings.warn(
            "The method canonical has been renamed documents. This alias "
            "may be removed in a future release.")
        yield from self.documents(fill=fill, strict_order=strict_order)

    def get_file_list(self, resource):
        """
        Fetch filepaths of external files associated with this Run.

        This method is not defined on RemoteBlueskyRun because the filepaths
        may not be meaningful on a remote machine.

        This method should be considered experimental. It may be changed or
        removed in a future release.
        """
        files = []
        handler = self.fillers['yes'].get_handler(resource)

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
    get_resources: callable
        Expected signature ``get_resources() -> Resources``
    lookup_resource_for_datum : callable
        Expected signature ``lookup_resource_for_datum(datum_id) -> resource_uid``
    get_datum_pages : callable
        Expected signature ``get_datum_pages(resource_uid) -> generator``
        where ``generator`` yields datum_page documents
    fillers : dict of Fillers
    transforms : Dict[str, Callable]
        A dict that maps any subset of the keys {start, stop, resource, descriptor}
        to a function that accepts a document of the corresponding type and
        returns it, potentially modified. This feature is for patching up
        erroneous metadata. It is intended for quick, temporary fixes that
        may later be applied permanently to the data at rest
        (e.g., via a database migration).
    metadata : dict
        passed through to base class
    include : list, optional
        Fields ('data keys') to include. By default all are included. This
        parameter is mutually exclusive with ``exclude``.
    exclude : list, optional
        Fields ('data keys') to exclude. By default none are excluded. This
        parameter is mutually exclusive with ``include``.
    sub_dict : {"data", "timestamps"}, optional
        Which sub-dict in the EventPage to use
    configuration_for : str
        The name of an object (e.g. device) whose configuration we want to
        read.
    **kwargs :
        Additional keyword arguments are passed through to the base class.
    """

    # def __dask_tokenize__(self):
    #     print('bill')
    #     intake_desc = self.describe()
    #     return ('Stream', intake_desc['metadata']['start']['uid'], metadata['name'])
    # opt-out of the persistence features of intake
    @property
    def has_been_persisted(self):
        return False

    @property
    def is_persisted(self):
        return False

    def get_persisted(self):
        raise KeyError("Does not support intake persistence")

    def persist(self, *args, **kwargs):
        raise NotImplementedError

    @property
    def _pmode(self):
        return 'never'

    @_pmode.setter
    def _pmode(self, val):
        ...

    container = 'bluesky-event-stream'
    version = '0.0.1'
    partition_access = True

    def __init__(self,
                 stream_name,
                 get_run_stop,
                 get_event_descriptors,
                 get_event_pages,
                 get_event_count,
                 get_resources,
                 get_resource,
                 lookup_resource_for_datum,
                 get_datum_pages,
                 fillers,
                 transforms,
                 metadata,
                 entry,
                 include=None,
                 exclude=None,
                 sub_dict="data",
                 configuration_for=None,
                 **kwargs):

        self._stream_name = stream_name
        self._get_event_descriptors = get_event_descriptors
        self._get_run_stop = get_run_stop
        self._get_event_pages = get_event_pages
        self._get_event_count = get_event_count
        self._get_resources = get_resources
        self._get_resource = get_resource
        self._lookup_resource_for_datum = lookup_resource_for_datum
        self._get_datum_pages = get_datum_pages
        self.fillers = fillers
        self._transforms = transforms
        self.urlpath = ''  # TODO Not sure why I had to add this.
        self._ds = None  # set by _open_dataset below
        self.include = include
        self.exclude = exclude
        if sub_dict not in {"data", "timestamps"}:
            raise ValueError(
                "The parameter 'sub_dict' controls where the xarray should "
                "contain the Events' 'data' (common case) or reading-specific "
                "'timestamps' (sometimes needed for hardware debugging). It "
                f"must be one of those two strings, not {sub_dict}.")
        self._configuration_for = configuration_for
        self._sub_dict = sub_dict
        self._partitions = None
        self.__entry = entry

        super().__init__(metadata=metadata, **kwargs)
        # turn off any attempts at persistence
        self._pmode = "never"
        self._run_stop_doc = metadata['stop']
        self._run_start_doc = metadata['start']
        self._load_header()
        logger.debug(
            "Created %s for stream name %r",
            self.__class__.__name__,
            self._stream_name)

    def _load_header(self):
        # TODO Add driver API to fetch only the descriptors of interest instead
        # of fetching all of them and then filtering.
        self._descriptors = d = [Descriptor(self._transforms['descriptor'](descriptor))
                                 for descriptor in self._get_event_descriptors()
                                 if descriptor.get('name') == self._stream_name]
        self.metadata.update({'descriptors': d})
        # TODO Should figure out a way so that self._resources doesn't have to
        # be all of the Run's resources.
        # TDOO Should we expose this in metadata as well? Since
        # _get_resources() only discovers new-style Resources that have a
        # run_start in them, leave it private for now.
        self._resources = [Resource(self._transforms['resource'](resource))
                           for resource in self._get_resources()]

        # get_run_stop() may return None if the document was never created due
        # to a critical failure or simply not yet emitted during a Run that is
        # still in progress. If it returns None, pass that through.
        if self._run_stop_doc is None:
            stop = self._get_run_stop()
            if stop is not None:
                self._run_stop_doc = s = Stop(self._transforms['stop'](stop))
                self.metadata.update({'stop': s})
        logger.debug(
            "Loaded %s for stream name %r",
            self.__class__.__name__,
            self._stream_name)

    def __repr__(self):
        try:
            out = (f"<{self.__class__.__name__} {self._stream_name!r} "
                   f"from Run {self._run_start_doc['uid'][:8]}...>")
        except Exception as exc:
            out = f"<{self.__class__.__name__} *REPR_RENDERING_FAILURE* {exc!r}>"
        return out

    def _open_dataset(self):
        self._load_header()
        if self._configuration_for is not None:
            self._ds = documents_to_xarray_config(
                object_name=self._configuration_for,
                sub_dict=self._sub_dict,
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
        else:
            self._ds = documents_to_xarray(
                sub_dict=self._sub_dict,
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
        self._load_header()
        datum_gens = [self._get_datum_pages(resource['uid'])
                      for resource in self._resources]
        event_gens = [list(self._get_event_pages(descriptor['uid']))
                      for descriptor in self._descriptors]
        self._partitions = list(
            _unfilled_partitions(self._run_start_doc, self._descriptors,
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
                self._partitions[i:i] = self._missing_datum(err.key, partition_size)
                return [filler(name, doc) for name, doc in self._partitions[i]]
        except IndexError:
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
        return intake.container.base.get_partition(
            self.url, self.http_args,
            self._source_id, self.container,
            partition)

    @property
    def config(self):
        objects = set()
        for d in self._descriptors:
            objects.update(set(d["object_keys"]))
        return intake.catalog.Catalog.from_dict(
            {object_name: self.configure_new(configuration_for=object_name)
             for object_name in objects}
        )

    @property
    def config_timestamps(self):
        objects = set()
        for d in self._descriptors:
            objects.update(set(d["object_keys"]))
        return intake.catalog.Catalog.from_dict(
            {object_name: self.configure_new(configuration_for=object_name,
                                             sub_dict="timestamps")
             for object_name in objects}
        )

    @property
    def timestamps(self):
        return self.configure_new(sub_dict="timestamps")

    def configure_new(self, **kwargs):
        """
        Return self or, if args are provided, some new instance of type(self).

        This is here so that the user does not have to remember whether a given
        variable is a BlueskyRun or an *Entry* with a Bluesky Run. In either
        case, ``obj()`` will return a BlueskyRun.
        """
        return self.__entry.get(**kwargs)


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

    def __repr__(self):
        try:
            out = (f"<{self.__class__.__name__} {self._stream_name!r} "
                   f"from Run {self._run_start_doc['uid'][:8]}...>")
        except Exception as exc:
            out = f"<{self.__class__.__name__} *REPR_RENDERING_FAILURE* {exc!r}>"
        return out


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


class SingleRunCache:
    """
    Collect the document from one Run and, when complete, provide a BlueskyRun.

    Parameters
    ----------
    handler_registry: dict, optional
        This is passed to the Filler or whatever class is given in the
        filler_class parameter below.

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
    transforms: dict
        A dict that maps any subset of the keys {start, stop, resource, descriptor}
        to a function that accepts a document of the corresponding type and
        returns it, potentially modified. This feature is for patching up
        erroneous metadata. It is intended for quick, temporary fixes that
        may later be applied permanently to the data at rest
        (e.g., via a database migration).

    Examples
    --------
    Subscribe to a document stream from within a plan.
    >>> def plan():
    ...     src = SingleRunCache()
    ...
    ...     @bluesky.preprocessors.subs_decorator(src.callback)
    ...     def inner_plan():
    ...         yield from bluesky.plans.rel_scan(...)
    ...         run = src.retrieve()
    ...         table = run.primary.read().to_dataframe()
    ...         ...
    ...
    ...     yield from inner_plan()

    """
    def __init__(self, *, handler_registry=None, root_map=None,
                 filler_class=event_model.Filler, transforms=None):

        self._root_map = root_map or {}
        self._filler_class = filler_class
        self._transforms = parse_transforms(transforms)
        if handler_registry is None:
            handler_registry = discover_handlers()
        self._handler_registry = parse_handler_registry(handler_registry)
        self.handler_registry = event_model.HandlerRegistryView(
            self._handler_registry)

        self._get_filler = functools.partial(
            self._filler_class,
            handler_registry=self.handler_registry,
            root_map=self._root_map,
            inplace=False)
        self._collector = deque()  # will contain (name, doc) pairs
        self._complete = False  # set to Run Start uid when stop doc is received
        self._run = None  # Cache BlueskyRun instance here.

    def callback(self, name, doc):
        """
        Subscribe to a document stream.
        """
        if self._complete:
            raise ValueError(
                "Already received 'stop' document. Expected one Run only.")
        if name == "stop":
            self._complete = doc["run_start"]
        self._collector.append((name, doc))

    def retrieve(self):
        """
        Return a BlueskyRun. If one is not ready, return None.
        """
        if not self._complete:
            return None
        if self._run is None:

            def gen_func():
                yield from self._collector

            # TODO in a future PR:
            # We have to mock up an Entry.
            # Can we avoid this after the Entry refactor?
            from types import SimpleNamespace
            _, start_doc = next(iter(self._collector))
            entry = SimpleNamespace(name=start_doc["uid"])

            self._run = BlueskyRunFromGenerator(
                gen_func, (), {}, get_filler=self._get_filler,
                transforms=self._transforms, entry=entry)
        return self._run

    def __repr__(self):
        # Either <SingleRunCache in progress>
        # or <SingleRunCache uid="...">
        if self._complete:
            return f"<SingleRunCache uid={self._complete}>"
        else:
            return "<SingleRunCache in progress>"


class BlueskyRunFromGenerator(BlueskyRun):

    def __init__(self, gen_func, gen_args, gen_kwargs, get_filler,
                 transforms, **kwargs):

        document_cache = DocumentCache()

        for item in gen_func(*gen_args, **gen_kwargs):
            document_cache(*item)

        assert document_cache.start_doc is not None

        def get_run_start():
            return document_cache.start_doc

        def get_run_stop():
            return document_cache.stop_doc

        def get_event_descriptors():
            return list(document_cache.descriptors.values())

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
            transforms=transforms,
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
        try:
            # compatibility with dask < 2
            if hasattr(out[k][0], 'shape'):
                out[k] = dask.array.stack(out[k])
            else:
                out[k] = dask.array.array(out[k])
        except NotImplementedError:
            # There are data structured that dask auto-chunking cannot handle,
            # such as an list of list of variable length. For now, let these go
            # out as plain numpy arrays. In the future we might make them dask
            # arrays with manual chunks.
            out[k] = numpy.asarray(out[k])
        except ValueError as err:
            # TEMPORARY EMERGENCY FALLBACK
            # If environment variable is set to anything but 0, work around
            # dask and return a numpy array.
            databroker_array_fallback = os.environ.get('DATABROKER_ARRAY_FALLBACK')
            if databroker_array_fallback != "0":
                out[k] = numpy.asarray(out[k])
                warnings.warn(
                    f"Creating a dask array raised an error. Because the "
                    f"environment variable DATABROKER_ARRAY_FALLBACK was set "
                    f"to {databroker_array_fallback} we have caught the error and "
                    f"fallen back to returning a numpy array instead. This may be "
                    f"very slow. The underlying issue should be resolved. The "
                    f"error was {err!r}.")
            else:
                raise

    return out


def _ft(timestamp):
    "format timestamp"
    if isinstance(timestamp, str):
        return timestamp
    # Truncate microseconds to miliseconds. Do not bother to round.
    return (datetime.fromtimestamp(timestamp)
            .strftime('%Y-%m-%d %H:%M:%S.%f'))[:-3]


def _xarray_to_event_gen(data_xarr, ts_xarr, page_size):
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
                warnings.warn(f"Skipping {entrypoint!r} which failed to load. "
                              f"Exception: {exc!r}")
                continue
            else:
                raise
        handler_registry[name] = handler_class

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


def parse_transforms(transforms):
    """
    Parse mapping of spec name to 'import path' into mapping to class itself.

    Parameters
    ----------
    transforms : collections.abc.Mapping or None
        A collections.abc.Mapping or subclass, that maps any subset of the
        keys {start, stop, resource, descriptor} to a function (or a string
        import path) that accepts a document of the corresponding type and
        returns it, potentially modified. This feature is for patching up
        erroneous metadata. It is intended for quick, temporary fixes that
        may later be applied permanently to the data at rest (e.g via a
        database migration).

    Examples
    --------
    Pass in name; get back actual class.

    >>> parse_transforms({'descriptor': 'package.module.function_name'})
    {'descriptor': <package.module.function_name>}

    """
    transformable = {'start', 'stop', 'resource', 'descriptor'}

    if transforms is None:
        result = {key: _no_op for key in transformable}
        return result
    elif isinstance(transforms, collections.abc.Mapping):
        if len(transforms.keys() - transformable) > 0:
            raise NotImplementedError(f"Transforms for {transforms.keys() - transformable} "
                                      f"are not supported.")
        result = {}

        for name in transformable:
            transform = transforms.get(name)
            if isinstance(transform, str):
                module_name, _, class_name = transform.rpartition('.')
                function = getattr(importlib.import_module(module_name), class_name)
            elif transform is None:
                function = _no_op
            else:
                function = transform
            result[name] = function
        return result
    else:
        raise ValueError(f"Invalid transforms argument {transforms}. "
                         f"transforms must be None or a dictionary.")


# This determines the type of the class that you get on the
# client side.
intake.container.register_container('bluesky-run', RemoteBlueskyRun)
intake.container.register_container(
    'bluesky-event-stream', RemoteBlueskyEventStream)


def _concat_dataarray_pages(dataarray_pages):
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


def _event_page_to_dataarray_page(event_page, dims=None, coords=None):
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


def _dataarray_page_to_dataset_page(dataarray_page):

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
            shape = extract_shape(descriptor, key, filler_state.resource)
            # there is an un-determined size (-1) in the shape, abandon
            # lazy as it will not work
            if any(s <= 0 for s in shape):
                return dask.array.from_array(super().__call__(*args, **kwargs))
            else:
                dtype = extract_dtype(descriptor, key)
                load_chunk = dask.delayed(super().__call__)(*args, **kwargs)
                return dask.array.from_delayed(load_chunk, shape=shape, dtype=dtype)

    return Subclass


# This adds a 'delayed' option to event_model.Filler's `coerce` parameter.
# By adding it via plugin, we avoid adding a dask.array dependency to
# event-model and we keep the fiddly hacks into extract_shape here in
# databroker, a faster-moving and less fundamental library than event-model.
event_model.register_coercion('delayed', coerce_dask)


def extract_shape(descriptor, key, resource=None):
    """
    Patch up misreported 'shape' metadata in old documents.

    This uses heuristcs to guess if the shape looks wrong and determine the
    right one. Once historical data has been fixed, this function will be
    reused to::

    return descriptor['data_keys'][key]['shape']
    """
    data_key = descriptor['data_keys'][key]
    if resource is not None:
        if "frame_per_point" in resource.get("resource_kwargs", {}):
            # This is a strong signal that the correct num_images value is here.
            num_images = resource["resource_kwargs"]["frame_per_point"]
        else:
            # Otherwise try to find something ending in 'num_images' in the
            # configuration dict associated with this device.
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
    # Work around bug in https://github.com/bluesky/ophyd/pull/746
    # Broken ophyd reports (x, y, 0). We want (num_images, y, x).
    if len(data_key['shape']) == 3 and data_key['shape'][-1] == 0:
        x, y, _ = data_key['shape']
        shape = (num_images, y, x)
    else:
        shape = data_key['shape']
    if num_images == -1:
        # Along this path, we have no way to make dask work. The calling code
        # will fall back to numpy.
        return shape
    else:
        # Along this path, we have inferred that a -1 in here is num_images,
        # extracted based on inspecting the resource or the descriptor,
        # and we should replace -1 with that.
        shape_ = []
        for item in shape:
            if item == -1:
                shape_.append(num_images)
            else:
                shape_.append(item)
        return shape_


def extract_dtype(descriptor, key):
    """
    Work around the fact that we currently report jsonschema data types.
    """
    reported = descriptor['data_keys'][key]['dtype']
    if reported == 'array':
        return float  # guess!
    else:
        return reported


def _no_op(doc):
    return doc


# This comes from the old databroker.core from before intake-bluesky was merged
# in. It apparently was useful for back-compat at some point.
from databroker._core import Header  # noqa: 401, 402
