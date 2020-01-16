# generate_data.py
import logging
import tempfile
from suitcase.mongo_embedded import Serializer
from bluesky import RunEngine
from bluesky.plans import count
from ophyd.sim import det
import pymongo
import uuid
import time


RE = RunEngine()

uri = f'mongodb://localhost:27017/databroker-test-{uuid.uuid4()}'
database = pymongo.MongoClient(uri).get_database()
with Serializer(database) as serializer:
    RE(count([det]), serializer)
    # time.sleep(5)
with Serializer(database) as serializer:
    RE(count([det], 3), serializer)
    # time.sleep(5)

logger = logging.getLogger('databroker')
logger.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setLevel('DEBUG')
logger.addHandler(handler)

from databroker._drivers.mongo_embedded import BlueskyMongoCatalog
catalog = BlueskyMongoCatalog(f'{directory}/*.msgpack')
