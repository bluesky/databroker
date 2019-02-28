import event_model
from functools import partial
import intake
import intake.catalog
import intake.catalog.local
import intake.source.base
import pymongo
import pymongo.errors

from .core import parse_handler_registry


class BlueskyMongoCatalog(intake.catalog.Catalog):
    def __init__(self, datastore_db, *, handler_registry=None,
                 query=None, **kwargs):
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
            Maps each asset spec to a handler class or a string specifying the
            module name and class name, as in (for example)
            ``{'SOME_SPEC': 'module.submodule.class_name'}``.
        query : dict, optional
            MongoDB query. Used internally by the ``search()`` method.
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

        if handler_registry is None:
            handler_registry = {}
        parsed_handler_registry = parse_handler_registry(handler_registry)
        self.filler = event_model.Filler(parsed_handler_registry)
        super().__init__(**kwargs)

    def _get_run_stop(self, run_uid):
        return self._db.header.find_one({'run_id': run_uid},
                                        {'stop': True,'_id': False})

    def _get_descriptors(self, run_uid):
        return self._db.header.find_one({'run_id': run_uid},
                                        {'descriptors': True, '_id': False})

    def _get_event_cursor(self, descriptor_uids, skip=0, limit=None):
        cursor = self._db.event.find({'descriptor': {'$in': descriptor_uids}},
                                     {'_id':False},
                                     sort=[('time.0', pymongo.ASCENDING)])
        cursor.skip(skip)
        if limit is not None:
            cursor = cursor.limit(limit)
        for doc in cursor:
            yield doc

    def _get_event_count(self, descriptor_uids):
        return (self._db.event.count_documents({'descriptor': {'$in':
                                               descriptor_uids}}))

    def _get_resource(self, run_uid, uid):
        resources_doc = self._db.header.find_one({'run_id': run_uid},
                                              {'resources': True, '_id':False})
        for doc in resources_doc:
            if doc['uid'] == uid:
                return doc

        raise ValueError(f"Could not find Resource with uid={uid}")

    def _get_datum(self, run_uid, datum_id):
        datum_cursor = self._db.datum.find({'run_id' : run_uid}, {'_id':False})

        for datum_page in datum_cursor:
            for datum in datum_page:
                if datum['datum_id'] = datum_id:
                    return datum
        raise ValueError(f"Could not find Datum with datum_id={datum_id}")

    def _get_datum_cursor(self, resource_uid):
        return self._db.datum.find({'run_id' : run_uid}, {'_id':False})

        self._schema = {}  # TODO This is cheating, I think.

    def _make_entries_container(self):
        catalog = self

        class Entries:
            "Mock the dict interface around a MongoDB query result."
            def _doc_to_entry(self, run_start_doc):
                uid = run_start_doc['uid']
                entry_metadata = {'start': run_start_doc,
                                  'stop': catalog._get_run_stop(uid)}
                args = dict(
                    run_start_doc=run_start_doc,
                    get_run_stop=partial(catalog._get_run_stop, uid),
                    get_event_descriptors=partial(catalog._get_descriptors,
                                                  uid),
                    get_event_cursor=catalog._get_event_cursor,
                    get_event_count=catalog._get_event_count,
                    get_resource=catalog._get_resource,
                    get_datum=catalog._get_datum,
                    get_datum_cursor=catalog._get_datum_cursor,
                    filler=catalog.filler)
                return intake.catalog.local.LocalCatalogEntry(
                    name=run_start_doc['uid'],
                    description={},  # TODO
                    driver='intake_bluesky.core.RunCatalog',
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
                cursor = catalog._db.header.find(catalog._query,
                        sort=[('start.time', pymongo.DESCENDING)])
                for run_start_doc in cursor:
                    yield run_start_doc['uid']

            def values(self):
                cursor = catalog._db.header.find(
                    catalog._query, sort=[('start.time', pymongo.DESCENDING)])
                for run_start_doc in cursor:
                    yield self._doc_to_entry(run_start_doc)

            def items(self):
                cursor = catalog._db.header.find(
                    catalog._query, sort=[('start.time', pymongo.DESCENDING)])
                for run_start_doc in cursor:
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
                        cursor = (catalog._db.header.find(query)
                            .sort('start.time', pymongo.DESCENDING).limit(name))
                        *_, run_start_doc = cursor
                    else:
                        # Interpret positive N as
                        # "most recent entry with scan_id == N".
                        query = {'$and': [catalog._query, {'scan_id': name}]}
                        cursor = (catalog._db.header.find(query)
                                  .sort('start.time', pymongo.DESCENDING)
                                  .limit(1))
                        run_start_doc, = cursor
                else:
                    query = {'$and': [catalog._query, {'uid': name}]}
                    run_start_doc = catalog._db.header.find_one(query)
                if run_start_doc is None:
                    raise KeyError(name)
                return self._doc_to_entry(run_start_doc)

            def __contains__(self, key):
                # Avoid iterating through all entries.
                try:
                    self[key]
                except KeyError:
                    return False
                else:
                    return True

        return Entries()

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
        cat = type(self)(
            datastore_db=self._db,
            query=query,
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


intake.registry['mongo_metadatastore'] = BlueskyMongoCatalog
