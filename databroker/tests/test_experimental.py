from pathlib import Path
from tiled.client import from_config

from .server_ext import MongoAdapter
from .client_ext import submit_array
from .queries import scan_id

import mongomock
import pytest

import numpy
import pandas


def test_client(tmpdir):
    
    db_name = "tiled-dev-1"
    
    mongo_client = mongomock.MongoClient()
    tiled_dev_db = mongo_client[db_name]
    
    
    Path(tmpdir, db_name).mkdir()
    print(tmpdir)
    
    api_key = "abcde"
    config = {
        # "authentication": {"allow_anonymous_access": True},
        "authentication": {"single_user_api_key": api_key},
        "trees": [
            {
                "path": "/",
                "tree": MongoAdapter.from_uri,
                "args": {
                    "uri": f"mongodb://localhost:27017/{db_name}",
                    "directory": f"{tmpdir}/{db_name}",
                    }
                }
            ]
    }
    
    client = from_config(config, api_key=api_key)
    
    test_array = numpy.ones((5, 5))
    
    submit_array(
        client.context, test_array, {"scan_id": 1, "method": "A"}, ["BlueskyNode"], "image/png"
    )
    
    # print("searching for reconstructions corresponding to scan_id 1...")
    # results = client.search(scan_id(1))

    
    print(client)
    
def test_module(tmpdir):
    print("tempDir: ", tmpdir)