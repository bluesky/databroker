from tiled.client import from_tree

from ..experimental.server_ext import MongoAdapter
from ..experimental.queries import scan_id

import pytest

import numpy
import pandas


def test_create_array(tmpdir):

    api_key = "secret"

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )

    test_array = numpy.ones((5, 5))

    client.create_array(test_array, {"scan_id": 1, "method": "A"}, ["BlueskyNode"])

    results = client.search(scan_id(1))
    result_array = results.values_indexer[0].read()

    numpy.testing.assert_equal(result_array, test_array)


def test_create_dataframe(tmpdir):

    api_key = "secret"

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )

    dummy_array = numpy.ones((5, 5))

    data = {
        "Column1": dummy_array[0],
        "Column2": dummy_array[1],
        "Column3": dummy_array[2],
        "Column4": dummy_array[3],
        "Column5": dummy_array[4],
    }

    test_dataframe = pandas.DataFrame(data)

    client.create_dataframe(
        test_dataframe, {"scan_id": 1, "method": "A"}, ["BlueskyNode"]
    )

    results = client.search(scan_id(1))
    result_dataframe = results.values_indexer[0].read()

    pandas.testing.assert_frame_equal(result_dataframe, test_dataframe)
