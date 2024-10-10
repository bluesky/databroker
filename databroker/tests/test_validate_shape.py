from bluesky import RunEngine
from bluesky.plans import count
from ophyd.sim import img, DirectImage
from tiled.client import Context, from_context
from tiled.server.app import build_app

from ..mongo_normalized import MongoAdapter, BadShapeMetadata

import numpy as np
import pytest


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


@pytest.mark.parametrize(
    "shape,expected_shape",
    [
        ((10,), (11,)),
        ((10, 20), (10, 21)),
        ((10, 20), (10, 19)),
        ((10, 20, 30), (10, 21, 30)),
        ((10, 20, 30), (10, 20, 31)),
        ((20, 20, 20, 20), (20, 21, 20, 22)),
    ],
)
def test_padding(tmpdir, shape, expected_shape):
    adapter = MongoAdapter.from_mongomock()

    direct_img = DirectImage(
        func=lambda: np.array(np.ones(shape)), name="direct", labels={"detectors"}
    )
    direct_img.img.name = "img"

    with Context.from_app(build_app(adapter), token_cache=tmpdir) as context:
        client = from_context(context)

        def post_document(name, doc):
            if name == "descriptor":
                doc["data_keys"]["img"]["shape"] = expected_shape

            client.post_document(name, doc)

        RE = RunEngine()
        RE.subscribe(post_document)
        (uid,) = RE(count([direct_img]))
        assert client[uid]["primary"]["data"]["img"][0].shape == expected_shape


@pytest.mark.parametrize(
    "shape,expected_shape",
    [
        ((10,), (11, 12)),
        ((10, 20), (10, 200)),
        ((20, 20, 20, 20), (20, 21, 20, 200)),
        ((10, 20), (5, 20)),
    ],
)
def test_default_validate_shape(tmpdir, shape, expected_shape):
    adapter = MongoAdapter.from_mongomock()

    direct_img = DirectImage(
        func=lambda: np.array(np.ones(shape)), name="direct", labels={"detectors"}
    )
    direct_img.img.name = "img"

    with Context.from_app(build_app(adapter), token_cache=tmpdir) as context:
        client = from_context(context)

        def post_document(name, doc):
            if name == "descriptor":
                doc["data_keys"]["img"]["shape"] = expected_shape

            client.post_document(name, doc)

        RE = RunEngine()
        RE.subscribe(post_document)
        (uid,) = RE(count([direct_img]))
        with pytest.raises(BadShapeMetadata):
            client[uid]["primary"]["data"]["img"][:]
