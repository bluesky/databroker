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
    from tiled.client import Context, from_context
    from tiled.server.app import build_app

    tree = MongoAdapter.from_mongomock()
    app = build_app(tree)
    context = Context.from_app(app)
    client = from_context(context)
    return client
