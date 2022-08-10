import string

import dask.array
import dask.dataframe
import numpy
import pandas
import sparse
from tiled.client import from_tree
from tiled.queries import (
    Contains,
    Comparison,
    Eq,
    FullText,
    In,
    Key,
    NotEq,
    NotIn,
    Regex,
)
from tiled.structures.sparse import COOStructure

from ..experimental.server_ext import MongoAdapter


API_KEY = "secret"


def test_write_array(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    test_array = numpy.ones((5, 7))

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    client.write_array(test_array, metadata=metadata, specs=specs)

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_array = result.read()

    numpy.testing.assert_equal(result_array, test_array)
    assert result.metadata == metadata
    # TODO In the future this will be accessible via result.specs.
    assert result.item["attributes"]["specs"] == specs


def test_write_dataframe(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    dummy_array = numpy.ones((5, 7))

    data = {
        "Column1": dummy_array[0],
        "Column2": dummy_array[1],
        "Column3": dummy_array[2],
        "Column4": dummy_array[3],
        "Column5": dummy_array[4],
    }

    test_dataframe = pandas.DataFrame(data)
    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]

    client.write_dataframe(test_dataframe, metadata=metadata, specs=specs)

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_dataframe = result.read()

    pandas.testing.assert_frame_equal(result_dataframe, test_dataframe)
    # slicing into DataFrameClient returns ArrayClient
    result_array = result["Column1"][:]
    assert numpy.array_equal(result_array, dummy_array[0])
    assert result.metadata == metadata
    # TODO In the future this will be accessible via result.specs.
    assert result.item["attributes"]["specs"] == specs


def test_queries(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    keys = list(string.ascii_lowercase)

    for letter, number in zip(keys, range(26)):
        metadata = {"letter": letter, "number": number}
        array = number * numpy.ones(10)

        client.write_array(array, metadata=metadata)

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
    test6 = client.search(NotEq("letter", "a"))
    # The first result should not be "a"
    assert test6.values()[0].metadata["letter"] != "a"

    test7 = client.search(In("letter", ["a", "b"]))
    numpy.testing.assert_equal(
        test7.values()[0].read(), test7.values()[0].metadata["number"] * numpy.ones(10)
    )
    numpy.testing.assert_equal(
        test7.values()[1].read(), test7.values()[1].metadata["number"] * numpy.ones(10)
    )

    test8 = client.search(NotIn("letter", ["a"]))
    # The first result should not be "a"
    assert test8.values()[0].metadata["letter"] != "a"


def test_delete(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    # For dataframes
    dummy_array = numpy.ones((5, 5))

    data = {
        "Column1": dummy_array[0],
        "Column2": dummy_array[1],
        "Column3": dummy_array[2],
        "Column4": dummy_array[3],
        "Column5": dummy_array[4],
    }

    test_dataframe = pandas.DataFrame(data)

    x = client.write_dataframe(
        test_dataframe, metadata={"scan_id": 1, "method": "A"}, specs=["BlueskyNode"]
    )

    del client[x.item["id"]]

    assert x.item["id"] not in client

    # For arrays
    test_array = numpy.ones((5, 5))

    y = client.write_array(
        test_array, metadata={"scan_id": 1, "method": "A"}, specs=["BlueskyNode"]
    )

    del client[y.item["id"]]

    assert y.item["id"] not in client


def test_write_array_chunked(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    a = dask.array.arange(24).reshape((4, 6)).rechunk((2, 3))

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    client.write_array(a, metadata=metadata, specs=specs)

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_array = result.read()

    numpy.testing.assert_equal(result_array, a.compute())
    assert result.metadata == metadata
    assert result.specs == specs


def test_write_dataframe_partitioned(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    data = {f"Column{i}": (1 + i) * numpy.ones(10) for i in range(5)}
    df = pandas.DataFrame(data)
    ddf = dask.dataframe.from_pandas(df, npartitions=3)
    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]

    client.write_dataframe(ddf, metadata=metadata, specs=specs)

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_dataframe = result.read()

    pandas.testing.assert_frame_equal(result_dataframe, df)
    assert result.metadata == metadata
    # TODO In the future this will be accessible via result.specs.
    assert result.item["attributes"]["specs"] == specs


def test_write_sparse_full(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    coo = sparse.COO(coords=[[0, 1], [2, 3]], data=[3.8, 4.0], shape=(4, 4))

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    client.write_sparse(
        coords=coo.coords,
        data=coo.data,
        shape=coo.shape,
        metadata=metadata,
        specs=specs,
    )

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_array = result.read()

    numpy.testing.assert_equal(result_array.todense(), coo.todense())
    assert result.metadata == metadata
    assert result.specs == specs


def test_write_sparse_chunked(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    N = 5
    x = client.new(
        "sparse",
        COOStructure(shape=(2 * N,), chunks=((N, N),)),
        metadata=metadata,
        specs=specs,
    )
    x.write_block(coords=[[2, 4]], data=[3.1, 2.8], block=(0,))
    x.write_block(coords=[[0, 1]], data=[6.7, 1.2], block=(1,))

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_array = result.read()
    assert numpy.array_equal(
        result_array.todense(),
        sparse.COO(
            coords=[[2, 4, N + 0, N + 1]], data=[3.1, 2.8, 6.7, 1.2], shape=(10,)
        ).todense(),
    )

    # numpy.testing.assert_equal(result_array, sparse.COO(coords=[0, 1, ]))
    assert result.metadata == metadata
    assert result.specs == specs
