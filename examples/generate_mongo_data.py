# generate_data.py
import logging
import tempfile
from suitcase.mongo_normalized import Serializer
from bluesky import RunEngine
from bluesky.plans import count
from ophyd.sim import det
import uuid


RE = RunEngine()

mds = f'mongodb://localhost:27017/databroker-test-{uuid.uuid4()}'
fs = f'mongodb://localhost:27017/databroker-test-{uuid.uuid4()}'
serializer = Serializer(mds, fs)
RE(count([det]), serializer)
RE(count([det], 3), serializer)

logger = logging.getLogger('databroker')
logger.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setLevel('DEBUG')
logger.addHandler(handler)

from databroker._drivers.mongo_normalized import BlueskyMongoCatalog
catalog = BlueskyMongoCatalog(f'{directory}/*.msgpack')
