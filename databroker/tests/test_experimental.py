from tiled.client import from_tree
from tiled.queries import Contains, Comparison, Eq, FullText, Key, Regex

from ..experimental.server_ext import MongoAdapter

import numpy
import pandas
import string
import xarray


def test_write_array(tmpdir):

    api_key = "secret"

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )

    test_array = numpy.ones((5, 5))

    client.write_array(test_array, {"scan_id": 1, "method": "A"}, ["BlueskyNode"])

    results = client.search(Key("scan_id") == 1)
    result_array = results.values().first().read()

    numpy.testing.assert_equal(result_array, test_array)


def test_write_dataframe(tmpdir):

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

    client.write_dataframe(
        test_dataframe, {"scan_id": 1, "method": "A"}, ["BlueskyNode"]
    )

    results = client.search(Key("scan_id") == 1)
    result_dataframe = results.values().first().read()

    pandas.testing.assert_frame_equal(result_dataframe, test_dataframe)


def test_write_dataarray(tmpdir):

    import dask.array

    api_key = "secret"

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )

    array = numpy.random.random((10, 10))

    test_xarray = xarray.DataArray(
        xarray.Variable(
            data=dask.array.from_array(array),
            dims=["x", "y"],
            attrs={"thing": "stuff"},
        ),
        coords={
            "x": dask.array.arange(len(array)),
            "y": 10 * dask.array.arange(len(array)),
        },
        attrs={"scan_id": 1, "method": "A"},
    )

    client.write_dataarray(test_xarray, specs=["BlueskyNode"])

    results = client.search(Key("attrs.scan_id") == 1)
    result_dataarray = results.values().first().read().values
    numpy.testing.assert_equal(result_dataarray, array)


def test_write_dataset(tmpdir):

    import dask.array

    api_key = "secret"

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )

    array = numpy.random.random((10, 10))
    test_dataset = xarray.Dataset(
        {
            "image": xarray.DataArray(
                xarray.Variable(
                    data=dask.array.from_array(array),
                    dims=["x", "y"],
                    attrs={"thing": "stuff"},
                ),
                coords={
                    "x": dask.array.arange(len(array)),
                    "y": 10 * dask.array.arange(len(array)),
                },
            ),
            "z": xarray.DataArray(data=dask.array.ones((len(array),))),
        }
    )

    client.write_dataset(test_dataset, specs=["BlueskyNode"])


def test_queries(tmpdir):

    api_key = "secret"

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )

    keys = list(string.ascii_lowercase)

    for letter, number in zip(keys, range(26)):
        metadata = {"letter": letter, "number": number}
        array = number * numpy.ones(10)

        client.write_array(array, metadata)

    test1 = client.search(Eq("letter", "a"))
    numpy.testing.assert_equal(
        test1.values()[0].read(), test1.values()[0].metadata["number"] * numpy.ones(10)
    )
    test2 = client.search(Contains("number", 1))
    numpy.testing.assert_equal(
        test2.values()[0].read(), test2.values()[0].metadata["number"] * numpy.ones(10)
    )
    test3 = client.search(Comparison("gt", "number", 24))
    numpy.testing.assert_equal(
        test3.values()[0].read(), test3.values()[0].metadata["number"] * numpy.ones(10)
    )
    test4 = client.search(FullText("y"))
    numpy.testing.assert_equal(
        test4.values()[0].read(), test4.values()[0].metadata["number"] * numpy.ones(10)
    )
    test5 = client.search(Regex("letter", "^c$"))
    numpy.testing.assert_equal(
        test5.values()[0].read(), test5.values()[0].metadata["number"] * numpy.ones(10)
    )
