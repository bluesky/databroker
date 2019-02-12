from bluesky import RunEngine
RE = RunEngine({})
from ophyd.sim import det
from bluesky.plans import count
from suitcase.jsonl import Serializer
serializer = Serializer('')
RE(count([det]), serializer)
serializer.close()
serializer = Serializer('')
RE(count([det]), serializer)
serializer.close()
