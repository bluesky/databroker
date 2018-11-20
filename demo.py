# DATA ACQUISITION
import pymongo
from bluesky import RunEngine
from bluesky.plans import count
from ophyd.sim import det


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


uri = 'mongodb://localhost:27017/test1'
RE = RunEngine({})
RE.subscribe(MongoInsertCallback(uri))
uid, = RE(count([det], 20))


# DATA ACCESS

from intake_bluesky import MongoMetadataStoreCatalog


mds = MongoMetadataStoreCatalog(uri)
run = mds[uid]
print(run.primary.read())
print(run.primary.read_slice(slice(7, 10)))
for chunk in run.primary.read_chunked():
    print(chunk)
for chunk in run.primary.read_chunked(chunks=4):
    print(chunk)
print(run.primary.det.read())
print(run.primary.det.read_slice(slice(7, 10)))
for chunk in run.primary.det.read_chunked():
    print(chunk)
for chunk in run.primary.det.read_chunked(chunks=4):
    print(chunk)
