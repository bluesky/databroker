import pathlib
import shutil

from bluesky.plans import count
from databroker._drivers.jsonl import BlueskyJSONLCatalog
import intake
from ophyd.sim import img
import pytest
from suitcase.jsonl import Serializer


def test_relative_root_map(RE, tmpdir):
    """
    When a Run has no RunStop document, whether because it does not exist yet
    or because the Run was interrupted in a critical way and never completed,
    we expect the field for 'stop' to contain None.
    """
    directory = str(tmpdir)

    serializer = Serializer(directory)
    RE(count([img]), serializer)
    serializer.close()
    dest = shutil.copytree(img.save_path, pathlib.Path(directory, 'external_data'))
    relative_d = str(pathlib.Path(dest.relative_to(directory)))
    root_map = {img.save_path: relative_d}

    # At this point root map maps the original absolute path to one relative to
    # the diretory containing the catalog.

    CATALOG_FILE = f"""
sources:
  test_relative_root_map:
    driver: bluesky-jsonl-catalog
    args:
      paths:
        - {directory}/*.jsonl
      root_map:
        {img.save_path}: {relative_d}"""
    catalog_path = str(pathlib.Path(directory, "catalog.yml"))
    with open(catalog_path, "w") as file:
        file.write(CATALOG_FILE)

    catalog = intake.open_catalog(catalog_path)
    subcatalog = catalog["test_relative_root_map"]()
    # At init time, Broker should resolve the relative path to an absolute one.
    assert subcatalog.root_map[img.save_path] == str(dest)

    # But it can only do this if it has a catalog *file* to interpret the path
    # relative to.
    with pytest.raises(ValueError):
        BlueskyJSONLCatalog(f'{directory}/*.jsonl', root_map=root_map)
