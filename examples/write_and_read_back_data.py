"""
This example is intended to be used with the docker-compose deployment.

TILED_SINGLE_USER_API_KEY=secret docker-compose up

"""

from bluesky import RunEngine
from bluesky.plans import scan
from ophyd.sim import det, motor
import suitcase.mongo_normalized
from tiled.client import from_uri


RE = RunEngine()

# Write data directly into MongoDB.
mongo_uri = "mongodb://localhost:27017/example_database"
serializer = suitcase.mongo_normalized.Serializer(mongo_uri, mongo_uri)
RE.subscribe(serializer)

# Access data via the Tiled service.
c = from_uri("http://localhost:8000?api_key=secret")

# Take some data.
RE(scan([det], motor, -1, 1, 3))
RE(scan([det], motor, -1, 1, 3))
RE(scan([det], motor, -1, 1, 3))

print(c)
print(c.values().last())
