import io
import os
from pathlib import Path
from shutil import copyfileobj
import sys
import zipfile

import appdirs
import databroker
from databroker_pack import unpack_inplace
import requests
from tqdm.auto import tqdm  # automatically chooses tqdm.tqdm or tqdm.notebook
from tqdm.utils import CallbackIOWrapper


DEFAULT_DATA_DIR = Path(appdirs.user_data_dir(), "bluesky_tutorial_data")
data_dir = os.getenv("BLUESKY_TUTORIAL_DATA", DEFAULT_DATA_DIR)


def _extractall_with_progress_bar(source, dest):
    "Unzip source into dest, updating a progress bar as we go."
    # Derived from https://stackoverflow.com/a/65513860rchive.extractall(directory)
    dest = Path(dest).expanduser()
    with zipfile.ZipFile(source) as zipf, tqdm(
        desc="Extracting",
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
        total=sum(getattr(i, "file_size", 0) for i in zipf.infolist()),
    ) as pbar:
        os.makedirs(dest, exist_ok=True)
        for i in zipf.infolist():
            if not getattr(i, "file_size", 0):  # directory
                zipf.extract(i, os.fspath(dest))
            else:
                with zipf.open(i) as fi, open(os.fspath(dest / i.filename), "wb") as fo:
                    copyfileobj(CallbackIOWrapper(pbar.update, fi), fo)
    print(f"Extracted internal data files to {dest}", file=sys.stderr)


def _download_with_progress_bar(response, buffer):
    "Stream the data from the response into the buffer, updating a progress bar as we go."
    # Derived from https://stackoverflow.com/a/37573701
    response.raise_for_status()
    total_size = int(response.headers.get("Content-Length", 0))
    block_size = 1024
    with tqdm(
        total=total_size, unit="iB", unit_scale=True, desc="Downloading"
    ) as progress_bar:
        for chunk in response.iter_content(block_size):
            progress_bar.update(len(chunk))
            buffer.write(chunk)


def _fetch_into_memory_and_unzip_to_disk(name, url):
    if name in databroker.catalog:
        return databroker.catalog[name]
    buffer = io.BytesIO()
    directory = Path(data_dir, name)
    with requests.get(url, stream=True) as response:
        _download_with_progress_bar(response, buffer)
    _extractall_with_progress_bar(buffer, directory)
    config_path = unpack_inplace(directory, name)
    print(
        f"Placed config file at {config_path} to add this catalog to databroker.catalog.\n",
        "Access it at any time via\n\n"
        "    import databroker\n"
        f"    databroker.catalog['{name}'].",
        file=sys.stderr,
    )
    # If the config directory did not exist at import time when
    # intake.catalog.default.load_combo_catalog() was run, it will never be
    # checked again. We need to explicitly add it.
    combo_catalog_path = databroker.catalog._catalogs[-1].path
    for ext in ["*.yml", "*.yaml"]:
        path = os.path.join(os.path.dirname(config_path), "*.yml")
        if path not in combo_catalog_path:
            combo_catalog_path.append(path)
    databroker.catalog.force_reload()


def fetch_BMM_example(version=1):
    if version != 1:
        raise ValueError("Only version 1 is known.")
    name = "bluesky-tutorial-BMM"
    url = "https://nsls2datasamples.blob.core.windows.net/bluesky-tutorial-example-data/BMM-example-v1.zip"
    return _fetch_into_memory_and_unzip_to_disk(name, url)


def fetch_RSOXS_example(version=1):
    if version != 1:
        raise ValueError("Only version 1 is known.")
    name = "bluesky-tutorial-RSOXS"
    url = "https://nsls2datasamples.blob.core.windows.net/bluesky-tutorial-example-data/RSOXS-example-v1.zip"
    return _fetch_into_memory_and_unzip_to_disk(name, url)
