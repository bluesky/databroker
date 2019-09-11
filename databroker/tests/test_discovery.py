from ..discovery import MergedCatalog, EntrypointsCatalog, V0Catalog
import copy
import os


def test_catalog_discovery():
    basedir = os.path.dirname(__file__)
    path = os.path.join(basedir, 'catalog_search')
    print(path)
    test_catalog = MergedCatalog([EntrypointsCatalog(paths=[path]),
                             V0Catalog(paths=[path])])

    assert 'v0' in test_catalog
    assert 'ep1' in test_catalog
