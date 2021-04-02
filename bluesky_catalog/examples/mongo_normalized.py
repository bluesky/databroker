import os

from bluesky_catalog.mongo_normalized import Catalog
from bluesky_catalog.server import router
from tiled.server.main import app


# Apply customizations to app.
app.include_router(router)

try:
    uri = os.environ["MONGO_URI"]
except KeyError:
    raise Exception("Must set environment variable MONGO_URI to use this module")
catalog = Catalog.from_uri(uri)
