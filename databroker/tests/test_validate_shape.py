from bluesky import RunEngine
from bluesky.plans import count
from ophyd.sim import img, DirectImage
from tiled.client import Context, from_context
from tiled.server.app import build_app

from ..mongo_normalized import MongoAdapter, BadShapeMetadata

import numpy as np
import pytest


@pytest.mark.parametrize(
    "shape,expected_shape",
    [
        ((10,), (11,)),  # Short by 1, 1d.
        ((10, 20), (10, 21)),  # Short by 1, 2d.
        ((10, 20, 30), (10, 21, 30)),  # Short by 1, 3d.
        ((10, 20, 30), (10, 20, 31)),  # Short by 1, 3d.
        ((10, 20), (10, 19)),  # Too-big by 1, 2d.
        ((20, 20, 20, 20), (20, 21, 20, 22)),  # 4d example.
        # TODO: Need to add some ragged test cases.
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
        # TODO: Need to add some ragged test cases.
    ],
)
def test_ragged_padding(tmpdir, shape, expected_shape):
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
    "shape,expected_chunks",
    [
        # default chunks shouldn't span files, this will fix read-timouts.
    ],
)
def test_default_chunking(tmpdir, shape, expected_shape):
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
    "shape,chunks",
    [
        # TODO: Need figure out test cases.
        # Accepts 'auto'.
        # suggested_chunks don’t line up perfectly with the data size.
        # supports chunk_shape in addition to chunks_list.
    ],
)
def test_custom_chunking(tmpdir, shape, expected_shape):
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
        ((10,), (11, 12)), # Different number of dimensions.
        ((10, 20), (10, 200)), # Dimension sizes differ by more than 2.
        ((20, 20, 20, 20), (20, 21, 20, 200)), # Dimension sizes differ by more than 2.
        ((10, 20), (5, 20)), # Data is bigger than expected.
    ],
)
def test_validate_shape_exceptions(tmpdir, shape, expected_shape):
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


def test_custom_validate_shape(tmpdir):
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
