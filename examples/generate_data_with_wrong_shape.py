"""
# Generate data with wrong shape in a demo MongoDB and Tiled server.

TILED_SINGLE_USER_API_KEY podman-compose up
python examples/generate_data_with_wrong_shape.py

# Try to load the data in the Tiled Python client.
# It will fail because the shape metadata is wrong.
from tiled.client import from_uri

c = from_uri('http://localhost:8000', api_key='secret')
c['raw'].values().last()['primary']['data']['img'][:]  # ERROR!

# The server logs should show:
# databroker.mongo_normalized.BadShapeMetadata: For data key img shape (5, 7) does not match expected shape (1, 11, 3).

# Run the shape-fixer CLI. Start with a dry run.
# The `--strict` mode ensures that errors are raised, not skipped.
databroker admin shape-fixer mongodb://localhost:27017/example_database --strict --dry-run
databroker admin shape-fixer mongodb://localhost:27017/example_database --strict

# The output should include something like this.
# (Of course, the uid(s) will be different.)

Edited 90b7ffa8-ba02-4163-a2aa-5f47d1eb322b primary: {'img': [1, 11, 3]} -> {'img': [5, 7]}
Migrating... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:00

# Back in the Python client, try loading again.
# There is no need to reconnect; just run this line again:
c['raw'].values().last()['primary']['data']['img'][:]  # Now it works!
"""



import numpy
from ophyd.sim import SynSignalWithRegistry
from bluesky import RunEngine
from bluesky.plans import count
from tiled.client import from_uri


RE = RunEngine()
client = from_uri("http://localhost:8000?api_key=secret")["raw"]


def post_document(name, doc):
    client.post_document(name, doc)


RE.subscribe(post_document)


class LyingDetector(SynSignalWithRegistry):
    def describe(self):
        res = super().describe()
        res["img"]["shape"] = (1, 11, 3)
        return res


img = LyingDetector(
    func=lambda: numpy.ones((5, 7), dtype=numpy.uint8),
    name="img",
    save_path="./data",
)
RE(count([img]))
