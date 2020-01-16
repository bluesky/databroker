# generate_data.py
import logging
import tempfile
from suitcase.msgpack import Serializer
from bluesky import RunEngine
from bluesky.plans import count
from ophyd.sim import det


RE = RunEngine()

directory = tempfile.TemporaryDirectory().name
with Serializer(directory) as serializer:
    RE(count([det]), serializer)
with Serializer(directory) as serializer:
    RE(count([det], 3), serializer)

logger = logging.getLogger('databroker')
logger.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setLevel('DEBUG')
logger.addHandler(handler)

from databroker._drivers.msgpack import BlueskyMsgpackCatalog
catalog = BlueskyMsgpackCatalog(f'{directory}/*.msgpack')
