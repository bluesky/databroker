"""
This example is intended to be used with the docker-compose deployment.

TILED_SINGLE_USER_API_KEY=secret docker-compose up

"""

from bluesky import RunEngine
from bluesky.plans import scan
from ophyd.sim import det, motor
from tiled.client import from_uri


RE = RunEngine()

# Connect to the Tiled service.
c = from_uri("http://localhost:8000?api_key=secret")

RE.subscribe(c.post_document)

# Take some data.
RE(scan([det], motor, -1, 1, 3))
RE(scan([det], motor, -1, 1, 3))
RE(scan([det], motor, -1, 1, 3))

print(c)
print(c.values().last())
