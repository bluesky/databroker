import intake.catalog
import intake.catalog.entry
import intake.source.base
import pymongo
import pymongo.errors


class FacilityCatalog(intake.catalog.Catalog):
    "spans multiple MongoDB instances"
    ...


class MetadataStoreCatalog(intake.catalog.Catalog):
    "represents one MongoDB instance"
    def __init__(self, uri, *, query=None, **kwargs):
        # All **kwargs are passed up to base class. TODO: spell them out
        # explicitly.
        self._uri = uri
        self._query = query
        self._client = pymongo.MongoClient(uri)
        try:
            # Called with no args, get_database() returns the database
            # specified in the uri --- or raises if there was none. There is no
            # public method for checking this in advance, so we just catch the
            # error.
            self._database = self._cli.get_database()
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(
                "Invalid uri. Did you forget to include a database?") from err
        self._run_start_colllection = db.get_collection('run_start')
        self._run_stop_collection = db.get_collection('run_stop')
        self._event_descriptor_collection = db.get_collection('event_descriptor')
        self._event_collection = db.get_collection('event')
        self._query = query or {}
        super().__init__(**kwargs)
        # Note: We don't do anything with self._entries, which is defined by
        # the base class, because we fetch our entries lazily rather than
        # caching them in a dict.

    def _get_entry(self, name):
        query = {'$and': [self._query, {'uid': name}]}
        run_start_doc = self._run_start_col.find(query)
        return RunCatalog(run_start_doc=run_start_doc,
                          run_stop_doc=run_stop_doc,
                          event_descriptor_collection=event_descriptor_collection,
                          event_collection=event_collection)

    def __iter__(self):
        run_start_docs = self._run_start_col.find(
            self._query, sort=[('time', pymongo.DESCENDING)]))
        for run_start_doc in run_start_docs:
            yield (run_start_doc['uid'],
                   RunCatalog(run_start_doc=run_start_doc,
                              run_stop_doc=run_stop_doc,
                              event_descriptor_collection=event_descriptor_collection,
                              event_collection=event_collection))

    def close(self):
        self._client.close()

    def search(self, query, depth=1):
        if self.query:
            query = {'$and': [self._query, query]}
        cat = MetadataStoreCatalog(
            uri=self.uri,
            query=query,
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options)
        cat.metadata['search'] = {'query': query, 'upstream': self.name}
        return cat


class RunCatalog(intake.catalog.Catalog):
    "represents one MongoDB instance"
    def __init__(self, *,
                 run_start_doc,
                 run_stop_doc,
                 event_descriptor_collection,
                 event_collection,
                 **kwargs):
        # All **kwargs are passed up to base class. TODO: spell them out
        # explicitly.
        self._
        self._query = query
        self._client = pymongo.MongoClient(uri)
        try:
            # Called with no args, get_database() returns the database
            # specified in the uri --- or raises if there was none. There is no
            # public method for checking this in advance, so we just catch the
            # error.
            self._database = self._cli.get_database()
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(
                "Invalid uri. Did you forget to include a database?") from err
        self._runstart_col = db.get_collection('run_start')
        self._query = query or {}
        super().__init__(**kwargs)
        # Note: We don't do anything with self._entries, which is defined by
        # the base class, because we fetch our entries lazily rather than
        # caching them in a dict.



class FooSource(intake.source.base.DataSource):
    container = 'dataframe'
    name = 'foo'
    version = '0.0.1'
    partition_access = True

    def __init__(self, a, b, metadata=None):
        # Do init here with a and b
        super(FooSource, self).__init__(
            metadata=metadata
        )

    def _get_schema(self):
        return intake.source.base.Schema(
            datashape=None,
            dtype={'x': "int64", 'y': "int64"},
            shape=(None, 2),
            npartitions=2,
            extra_metadata=dict(c=3, d=4)
        )

    def _get_partition(self, i):
        # Return the appropriate container of data here
        return pd.DataFrame({'x': [1, 2, 3], 'y': [10, 20, 30]})

    def read(self):
        self._load_metadata()
        return pd.concat([self.read_partition(i) for i in self.npartitions])

    def _close(self):
        # close any files, sockets, etc
        pass
