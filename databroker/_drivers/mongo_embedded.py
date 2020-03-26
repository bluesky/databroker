import collections.abc
import event_model
from sys import maxsize
from functools import partial
import logging
import cachetools
import pymongo
import pymongo.errors

from ..core import Entry
from ..v2 import Broker


logger = logging.getLogger(__name__)


class _Entries(collections.abc.Mapping):
    "Mock the dict interface around a MongoDB query result."
    def __init__(self, catalog):
        self.catalog = catalog
        self.__cache = cachetools.LRUCache(1024)

    def cache_clear(self):
        self.__cache.clear()

    def _doc_to_entry(self, run_start_doc):

        header_doc = None
        uid = run_start_doc['uid']

        def get_header_field(field):
            nonlocal header_doc
            if header_doc is None:
                header_doc = self.catalog._db.header.find_one(
                                {'run_id': uid}, {'_id': False})
            if field in header_doc:
                if field in ['start', 'stop']:
                    return header_doc[field][0]
                else:
                    return header_doc[field]
            else:
                if field == 'resources':
                    return []
                elif field[0:6] == 'count_':
                    return 0
                else:
                    return None

        def get_resource(uid):
            resources = get_header_field('resources')
            for resource in resources:
                if resource['uid'] == uid:
                    return resource
            raise ValueError(f"Could not find Resource with uid={uid}")

        def lookup_resource_for_datum(datum_id):
            """ This method is likely very slow. """
            resources = [resource['uid']
                         for resource in get_header_field('resources')]
            datum_page = self.catalog._db.datum.find_one(
                    {'$and': [{'resource': {'$in': resources}},
                              {'datum_id': datum_id}]},
                    {'_id': False})
            for datum in event_model.unpack_datum_page(datum_page):
                if datum['datum_id'] == datum_id:
                    return datum['resource']
            raise ValueError(f"Could not find Datum with datum_id={datum_id}")

        def get_run_start():
            return run_start_doc

        def get_event_count(descriptor_uid):
            return get_header_field(f'count_{descriptor_uid}')

        entry_metadata = {'start': get_header_field('start'),
                          'stop': get_header_field('stop')}

        args = dict(
            get_run_start=get_run_start,
            get_run_stop=partial(get_header_field, 'stop'),
            get_event_descriptors=partial(get_header_field, 'descriptors'),
            get_event_pages=self.catalog._get_event_pages,
            get_event_count=get_event_count,
            get_resource=get_resource,
            get_resources=partial(get_header_field, 'resources'),
            lookup_resource_for_datum=lookup_resource_for_datum,
            get_datum_pages=self.catalog._get_datum_pages,
            get_filler=self.catalog._get_filler,
            transforms=self.catalog._transforms)
        return Entry(
            name=run_start_doc['uid'],
            description={},  # TODO
            driver='databroker.core.BlueskyRun',
            direct_access='forbid',  # ???
            args=args,
            cache=None,  # ???
            parameters=[],
            metadata=entry_metadata,
            catalog_dir=None,
            getenv=True,
            getshell=True,
            catalog=self.catalog)

    def __iter__(self):
        cursor = self.catalog._db.header.find(
                    self.catalog._query,
                    {'start.uid': True, '_id': False},
                    sort=[('start.time', pymongo.DESCENDING)])

        for doc in cursor:
            yield doc['start'][0]['uid']

    def _find_header_doc(self, name):
        # If this came from a client, we might be getting '-1'.
        try:
            N = int(name)
        except ValueError:
            query = {'$and': [self.catalog._query, {'uid': name}]}
            header_doc = self.catalog._db.header.find_one(query)
            if header_doc is None:
                regex_query = {
                    '$and': [self.catalog._query,
                             {'start.uid': {'$regex': f'{name}.*'}}]}
                matches = list(
                    self.catalog._db.header.find(regex_query).limit(10))
                if not matches:
                    raise KeyError(name)
                elif len(matches) == 1:
                    header_doc, = matches
                else:
                    match_list = '\n'.join(doc['uid'] for doc in matches)
                    raise ValueError(
                        f"Multiple matches to partial uid {name!r}. "
                        f"Up to 10 listed here:\n"
                        f"{match_list}")
        else:
            if N < 0:
                # Interpret negative N as "the Nth from last entry".
                query = self.catalog._query
                cursor = (self.catalog._db.header.find(query)
                          .sort('start.time', pymongo.DESCENDING)
                          .skip(-N - 1)
                          .limit(1))
                try:
                    header_doc, = cursor
                except ValueError:
                    raise IndexError(
                        f"Catalog only contains {len(self.catalog)} "
                        f"runs.")
            else:
                # Interpret positive N as
                # "most recent entry with scan_id == N".
                query = {'$and': [self.catalog._query, {'start.scan_id': N}]}
                cursor = (self.catalog._db.header.find(query)
                          .sort('start.time', pymongo.DESCENDING)
                          .limit(1))
                try:
                    header_doc, = cursor
                except ValueError:
                    raise KeyError(f"No run with scan_id={N}")
        if header_doc is None:
            raise KeyError(name)
        return header_doc

    def __getitem__(self, name):
        header_doc = self._find_header_doc(name)
        uid = header_doc['start'][0]['uid']
        try:
            entry = self.__cache[uid]
            logger.debug('Mongo Entries cache found %r', uid)
        except KeyError:
            entry = self._doc_to_entry(header_doc['start'][0])
            self.__cache[uid] = entry
        # The user has requested one specific Entry. In order to give them a
        # more useful object, 'get' the Entry for them. Note that if they are
        # expecting an Entry and try to call ``()`` or ``.get()``, that will
        # still work because BlueskyRun supports those methods and will just
        # return itself.
        return entry.get()  # an instance of BlueskyRun

    def __contains__(self, key):
        # Try the fast path first.
        if key in self.__cache:
            return True
        # Avoid paying for creating the Entry yet. Do just enough work decide
        # if we *can* create such an Entry.
        try:
            self._find_header_doc(key)
        except KeyError:
            return False
        else:
            return True

    def __len__(self):
        return len(self.catalog)


class BlueskyMongoCatalog(Broker):
    def __init__(self, datastore_db, *, handler_registry=None, root_map=None,
                 filler_class=event_model.Filler, query=None,
                 transforms=None, **kwargs):
        """
        This Catalog is backed by a MongoDB with an embedded data model.

        This embedded data model has three collections: header, event, datum.
        The header collection includes start, stop, descriptor, and resource
        documents. The event_pages are stored in the event colleciton, and
        datum_pages are stored in the datum collection.

        Parameters
        ----------
        datastore_db : pymongo.database.Database or string
            Must be a Database or a URI string that includes a database name.
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
        filler_class: type, optional
            This is Filler by default. It can be a Filler subclass,
            ``functools.partial(Filler, ...)``, or any class that provides the
            same methods as ``DocumentRouter``.
        query : dict, optional
            MongoDB query. Used internally by the ``search()`` method.
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
        name = 'bluesky-mongo-embedded-catalog'  # noqa

        if isinstance(datastore_db, str):
            self._db = _get_database(datastore_db)
        else:
            self._db = datastore_db
        self._query = query or {}
        self._root_map = root_map
        self._filler_class = filler_class

        super().__init__(handler_registry=handler_registry,
                         root_map=root_map, filler_class=filler_class,
                         transforms=transforms, **kwargs)

    def _get_event_pages(self, descriptor_uid, skip=0, limit=None):
        if limit is None:
            limit = maxsize

        page_cursor = self._db.event.find(
                            {'$and': [
                                {'descriptor': descriptor_uid},
                                {'last_index': {'$gte': skip}},
                                {'first_index': {'$lte': skip + limit}}]},
                            {'_id': False},
                            sort=[('last_index', pymongo.ASCENDING)])

        return page_cursor

    def _get_datum_pages(self, resource_uid, skip=0, limit=None):
        if limit is None:
            limit = maxsize

        page_cursor = self._db.datum.find(
                            {'$and': [
                                {'resource': resource_uid},
                                {'last_index': {'$gte': skip}},
                                {'first_index': {'$lte': skip + limit}}]},
                            {'_id': False},
                            sort=[('last_index', pymongo.ASCENDING)])

        return page_cursor

    def _make_entries_container(self):
        return _Entries(self)

    def _close(self):
        self._client.close()

    def __len__(self):
        return self._db.header.count_documents(self._query)

    def search(self, query):
        """
        Return a new Catalog with a subset of the entries in this Catalog.

        Parameters
        ----------
        query : dict
            MongoDB query.
        """
        query = dict(query)
        if query:
            query = {f"start.{key}": val for key, val in query.items()}
        if self._query:
            query = {'$and': [self._query, query]}
        cat = type(self)(
            datastore_db=self._db,
            query=query,
            handler_registry=self._handler_registry,
            transforms=self._transforms,
            root_map=self._root_map,
            filler_class=self._filler_class,
            name='search results',
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options)
        return cat


def _get_database(uri):
    client = pymongo.MongoClient(uri)
    try:
        # Called with no args, get_database() returns the database
        # specified in the client's uri --- or raises if there was none.
        # There is no public method for checking this in advance, so we
        # just catch the error.
        return client.get_database()
    except pymongo.errors.ConfigurationError as err:
        raise ValueError(
            f"Invalid client: {client} "
            f"Did you forget to include a database?") from err
