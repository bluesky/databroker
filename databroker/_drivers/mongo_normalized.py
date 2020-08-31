import collections.abc
import event_model
from functools import partial
import logging
import cachetools
import pymongo
import pymongo.errors

from bson.objectid import ObjectId, InvalidId
from ..core import to_event_pages, to_datum_pages, Entry
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
        uid = run_start_doc['uid']
        run_start_doc.pop('_id')
        entry_metadata = {'start': run_start_doc,
                          'stop': self.catalog._get_run_stop(uid)}

        def get_run_start():
            return run_start_doc

        args = dict(
            get_run_start=get_run_start,
            get_run_stop=partial(self.catalog._get_run_stop, uid),
            get_event_descriptors=partial(self.catalog._get_event_descriptors, uid),
            # 2500 was selected as the page_size because it worked well durring
            # benchmarks, for HXN data a full page had roughly 3500 events.
            get_event_pages=to_event_pages(self.catalog._get_event_cursor, 2500),
            get_event_count=self.catalog._get_event_count,
            get_resource=self.catalog._get_resource,
            get_resources=partial(self.catalog._get_resources, uid),
            lookup_resource_for_datum=self.catalog._lookup_resource_for_datum,
            # 2500 was selected as the page_size because it worked well durring
            # benchmarks.
            get_datum_pages=to_datum_pages(self.catalog._get_datum_cursor, 2500),
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
        find_kwargs = {'sort': [('time', pymongo.DESCENDING)]}
        find_kwargs.update(self.catalog._find_kwargs)
        cursor = self.catalog._run_start_collection.find(
            self.catalog._query, **find_kwargs)
        for run_start_doc in cursor:
            yield run_start_doc['uid']

    def __getitem__(self, name):
        run_start_doc = self._find_run_start_doc(name)
        uid = run_start_doc['uid']
        try:
            entry = self.__cache[uid]
            logger.debug('Mongo Entries cache found %r', uid)
        except KeyError:
            entry = self._doc_to_entry(run_start_doc)
            self.__cache[uid] = entry
        # The user has requested one specific Entry. In order to give them a
        # more useful object, 'get' the Entry for them. Note that if they are
        # expecting an Entry and try to call ``()`` or ``.get()``, that will
        # still work because BlueskyRun supports those methods and will just
        # return itself.
        return entry.get()  # an instance of BlueskyRun

    def _find_run_start_doc(self, name):
        # If this came from a client, we might be getting '-1'.
        collection = self.catalog._run_start_collection
        try:
            N = int(name)
        except ValueError:
            query = {'$and': [self.catalog._query, {'uid': name}]}
            run_start_doc = collection.find_one(query)
            if run_start_doc is None:
                regex_query = {
                    '$and': [self.catalog._query,
                             {'uid': {'$regex': f'^{name}'}}]}
                matches = list(collection.find(regex_query).limit(10))
                if not matches:
                    raise KeyError(name)
                elif len(matches) == 1:
                    run_start_doc, = matches
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
                cursor = (collection.find(query)
                          .sort('time', pymongo.DESCENDING)
                          .skip(-N - 1)
                          .limit(1))
                try:
                    run_start_doc, = cursor
                except ValueError:
                    raise IndexError(
                        f"Catalog only contains {len(self.catalog)} "
                        f"runs.")
            else:
                # Interpret positive N as
                # "most recent entry with scan_id == N".
                query = {'$and': [self.catalog._query, {'scan_id': N}]}
                cursor = (collection.find(query)
                          .sort('time', pymongo.DESCENDING)
                          .limit(1))
                try:
                    run_start_doc, = cursor
                except ValueError:
                    raise KeyError(f"No run with scan_id={N}")
        if run_start_doc is None:
            raise KeyError(name)
        return run_start_doc

    def __contains__(self, key):
        # Try the fast path first.
        if key in self.__cache:
            return True
        # Avoid paying for creating the Entry yet. Do just enough work decide
        # if we *can* create such an Entry.
        try:
            self._find_run_start_doc(key)
        except KeyError:
            return False
        else:
            return True

    def __len__(self):
        return len(self.catalog)


class BlueskyMongoCatalog(Broker):
    def __init__(self, metadatastore_db, asset_registry_db, *,
                 handler_registry=None, root_map=None,
                 filler_class=event_model.Filler, query=None,
                 find_kwargs=None, transforms=None, **kwargs):
        """
        This Catalog is backed by a pair of MongoDBs with "layout 1".

        This layout uses a separate Mongo collection per document type and a
        separate Mongo document for each logical document.

        Parameters
        ----------
        metadatastore_db : pymongo.database.Database or string
            Must be a Database or a URI string that includes a database name.
        asset_registry_db : pymongo.database.Database or string
            Must be a Database or a URI string that includes a database name.
        handler_registry : dict, optional
            This is passed to the Filler or whatever class is given in the
            ``filler_class`` parameter below.
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
            This is passed to Filler or whatever class is given in the
            ``filler_class`` parameter below.
            str -> str mapping to account for temporarily
            moved/copied/remounted files.  Any resources which have a ``root``
            in ``root_map`` will be loaded using the mapped ``root``.
        filler_class: type, optional
            This is Filler by default. It can be a Filler subclass,
            ``functools.partial(Filler, ...)``, or any class that provides the
            same methods as ``DocumentRouter``.
        query : dict, optional
            MongoDB query. Used internally by the ``search()`` method.
        find_kwargs : dict, optional
            Options passed to pymongo ``find``.
        transforms : Dict[str, Callable]
            A dict that maps any subset of the keys {start, stop, resource, descriptor}
            to a function that accepts a document of the corresponding type and
            returns it, potentially modified. This feature is for patching up
            erroneous metadata. It is intended for quick, temporary fixes that
            may later be applied permanently to the data at rest
            (e.g via a database migration).
        **kwargs :
            Additional keyword arguments are passed through to the base class,
            Catalog.
        """
        name = 'bluesky-mongo-catalog'  # noqa

        if isinstance(metadatastore_db, str):
            mds_db = _get_database(metadatastore_db)
        else:
            mds_db = metadatastore_db
        if isinstance(asset_registry_db, str):
            assets_db = _get_database(asset_registry_db)
        else:
            assets_db = asset_registry_db

        self._run_start_collection = mds_db.get_collection('run_start')
        self._run_stop_collection = mds_db.get_collection('run_stop')
        self._event_descriptor_collection = mds_db.get_collection('event_descriptor')
        self._event_collection = mds_db.get_collection('event')
        self._resource_collection = assets_db.get_collection('resource')
        self._datum_collection = assets_db.get_collection('datum')
        self._metadatastore_db = mds_db
        self._asset_registry_db = assets_db
        self._query = query or {}
        self._find_kwargs = find_kwargs or {}

        super().__init__(handler_registry=handler_registry,
                         root_map=root_map, filler_class=filler_class,
                         transforms=transforms, **kwargs)

    def _get_run_stop(self, run_start_uid):
        doc = self._run_stop_collection.find_one(
            {'run_start': run_start_uid},
            {'_id': False})
        return doc

    def _get_event_descriptors(self, run_start_uid):
        cursor = self._event_descriptor_collection.find(
            {'run_start': run_start_uid},
            {'_id': False},
            sort=[('time', pymongo.ASCENDING)])
        return list(cursor)

    def _get_event_cursor(self, descriptor_uid, skip=0, limit=None):
        cursor = (self._event_collection
                  .find({'descriptor': descriptor_uid},
                        {'_id': False},
                        sort=[('time', pymongo.ASCENDING)]))
        descriptor = self._event_descriptor_collection.find_one(
            {'uid': descriptor_uid})
        cursor.skip(skip)
        if limit is not None:
            cursor = cursor.limit(limit)
        external_keys = {k for k, v in descriptor['data_keys'].items()
                         if 'external' in v}
        for doc in cursor:
            doc['filled'] = {k: False for k in external_keys}
            yield doc

    def _get_event_count(self, descriptor_uid):
        return self._event_collection.count_documents(
            {'descriptor': descriptor_uid})

    def _get_resources(self, run_start_uid):
        return list(self._resource_collection.find(
            {'run_start': run_start_uid}, {'_id': False}))

    def _get_resource(self, uid):
        doc = self._resource_collection.find_one({'uid': uid}, {'_id': False})

        # Some old resource documents don't have a 'uid' and they are
        # referenced by '_id'.
        if doc is None:
            try:
                _id = ObjectId(uid)
            except InvalidId:
                pass
            else:
                doc = self._resource_collection.find_one({'_id': _id}, {'_id': False})
                doc['uid'] = uid

        if doc is None:
            raise ValueError(f"Could not find Resource with uid={uid}")
        return doc

    def _lookup_resource_for_datum(self, datum_id):
        doc = self._datum_collection.find_one(
            {'datum_id': datum_id})
        if doc is None:
            raise ValueError(f"Could not find Datum with datum_id={datum_id}")
        return doc['resource']

    def _get_datum_cursor(self, resource_uid):
        self._schema = {}  # TODO This is cheating, I think.
        return self._datum_collection.find({'resource': resource_uid}, {'_id': False})

    def _make_entries_container(self):
        return _Entries(self)

    def __len__(self):
        return self._run_start_collection.count_documents(self._query)

    def _close(self):
        self._client.close()

    def search(self, query, **kwargs):
        """
        Return a new Catalog with a subset of the entries in this Catalog.

        Parameters
        ----------
        query : dict
            MongoDB query.
        **kwargs :
            Options passed through to the pymongo ``find()`` method
        """
        query = dict(query)
        if self._query:
            query = {'$and': [self._query, query]}
        cat = type(self)(
            metadatastore_db=self._metadatastore_db,
            asset_registry_db=self._asset_registry_db,
            query=query,
            find_kwargs=kwargs,
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

    def _get_serializer(self):
        "This is used internally by v1.Broker. It may be removed in future."
        from suitcase.mongo_normalized import Serializer
        return Serializer(self._metadatastore_db, self._asset_registry_db)

    def stats(self):
        "Access MongoDB storage statistics for this database."
        return self._run_start_collection.database.command("dbstats")


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
