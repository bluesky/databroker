from ..discovery import MergedCatalog, EntrypointsCatalog, V0Catalog
import copy
import os
import glob
import pytest

def test_catalog_discovery():
    basedir = os.path.dirname(__file__)
    path = os.path.join(basedir, 'catalog_search')
    collision_path = os.path.join(path, 'v0')


    test_catalog = MergedCatalog([EntrypointsCatalog(paths=[path]),
                                  V0Catalog(paths=[path])])

    assert 'v0' in test_catalog
    assert 'ep1' in test_catalog

    with pytest.warns(UserWarning):
        test_catalog = MergedCatalog([EntrypointsCatalog(paths=[path]),
                                      V0Catalog(paths=[path, collision_path])])

    assert 'v0' in test_catalog
    assert 'ep1' in test_catalog
