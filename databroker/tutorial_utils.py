import io
import os
from pathlib import Path
import zipfile

import appdirs
import databroker
from databroker_pack import unpack_inplace
import requests


DEFAULT_DATA_DIR = Path(appdirs.user_data_dir(), "bluesky_tutorial_data")


def fetch_BMM_example(version=1):
    if "bluesky-tutorial-BMM" in databroker.catalog:
        return
    URL = "https://nsls2datasamples.blob.core.windows.net/bluesky-tutorial-example-data/BMM-example-v1.zip"
    directory = Path(
        os.getenv("BLUESKY_TUTORIAL_DATA", DEFAULT_DATA_DIR),
        "BMM_example_v1"
    )
    with requests.get(URL) as response:
        download = io.BytesIO(response.content)
        with zipfile.ZipFile(download) as archive:
            archive.extractall(directory)
        unpack_inplace(directory, "bluesky-tutorial-BMM")
    databroker.catalog.force_reload()
