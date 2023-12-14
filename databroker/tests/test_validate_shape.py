from bluesky import RunEngine
from bluesky.plans import count
from ophyd.sim import img
from tiled.client import Context, from_context
from tiled.server.app import build_app

from ..mongo_normalized import MongoAdapter


def test_validate_shape(tmpdir):
    # custom_validate_shape will mutate this to show it has been called
    shapes = []

    def custom_validate_shape(key, data, expected_shape):
        shapes.append(expected_shape)
        return data

    adapter = MongoAdapter.from_mongomock(validate_shape=custom_validate_shape)

    with Context.from_app(build_app(adapter), token_cache=tmpdir) as context:
        client = from_context(context)

        def post_document(name, doc):
            client.post_document(name, doc)

        RE = RunEngine()
        RE.subscribe(post_document)
        (uid,) = RE(count([img]))
        assert not shapes
        client[uid]["primary"]["data"]["img"][:]
        assert shapes
