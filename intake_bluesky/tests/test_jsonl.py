import json
from bluesky import RunEngine
from bluesky.plans import scan
from ophyd.sim import direct_img, motor
from suitcase.jsonl import Serializer
from ..core import documents_to_xarray


def test_jsonl_smoke():
    RE = RunEngine({})
    serializer = Serializer('data/')
    RE.subscribe(serializer)
    RE(scan([direct_img], motor, -1, 1, 3))
    serializer.close()
    filename, = serializer.artifacts['stream_data']
    with open(filename) as file:
        docs = [json.loads(line) for line in file]
    _, start = docs[0]
    _, stop = docs[-1]
    descriptors = [doc for name, doc in docs if name == 'descriptor']
    events = [doc for name, doc in docs if name == 'event']
    documents_to_xarray(start, stop, descriptors, events)
