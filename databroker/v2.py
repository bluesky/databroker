from .client import CatalogOfBlueskyRuns


# This is retained for backward-compatibility as it was
# sometimes used for isinstance checks and can still
# serve that purpose.
Broker = CatalogOfBlueskyRuns


def temp():
    """
    Create a temporary Catalog backed by transient storage.

    This is intended for testing, teaching, an demos. The data does not
    persistent. Do not use this for anything important.
    """
    from .mongo_normalized import MongoAdapter
    from tiled.client import from_tree

    catalog = MongoAdapter.from_mongomock()  # service-side Catalog
    client = from_tree(catalog)  # client-side Catalog
    return client
