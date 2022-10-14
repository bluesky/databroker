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
from tiled.validation_registration import ValidationRegistry

from ..experimental.server_ext import MongoAdapter

from ..experimental.schemas import DocumentRevision


API_KEY = "secret"
validation_registry = ValidationRegistry()
validation_registry.register("SomeSpec", lambda *args, **kwargs: None)
validation_registry.register("AnotherSpec", lambda *args, **kwargs: None)
validation_registry.register("AnotherOtherSpec", lambda *args, **kwargs: None)


def test_write_array(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
    )

    test_array = numpy.ones((5, 7))

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    references = [{"label": "test", "url": "http://www.test.com"}]

    node = client.write_array(
        test_array, metadata=metadata, specs=specs, references=references
    )

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_array = result.read()

    numpy.testing.assert_equal(result_array, test_array)
    assert result.metadata == node.metadata == metadata
    assert result.specs == node.specs == specs
    assert result.references == node.references == references


def test_write_dataframe(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
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
    references = [{"label": "test", "url": "http://www.test.com"}]

    node = client.write_dataframe(
        test_dataframe, metadata=metadata, specs=specs, references=references
    )

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_dataframe = result.read()

    pandas.testing.assert_frame_equal(result_dataframe, test_dataframe)
    # slicing into DataFrameClient returns ArrayClient
    result_array = result["Column1"][:]
    assert numpy.array_equal(result_array, dummy_array[0])
    assert result.metadata == node.metadata == metadata
    assert result.specs == node.specs == specs
    assert result.references == node.references == references


def test_queries(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
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
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
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
        test_dataframe,
        metadata={"scan_id": 1, "method": "A"},
        specs=["SomeSpec"],
        references=[{"label": "test", "url": "http://www.test.com"}],
    )

    del client[x.item["id"]]

    assert x.item["id"] not in client

    # For arrays
    test_array = numpy.ones((5, 5))

    y = client.write_array(
        test_array,
        metadata={"scan_id": 1, "method": "A"},
        specs=["SomeSpec"],
        references=[{"label": "test", "url": "http://www.test.com"}],
    )

    del client[y.item["id"]]

    assert y.item["id"] not in client


def test_write_array_chunked(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
    )

    a = dask.array.arange(24).reshape((4, 6)).rechunk((2, 3))

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    references = [{"label": "test", "url": "http://www.test.com"}]
    client.write_array(a, metadata=metadata, specs=specs, references=references)

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_array = result.read()

    numpy.testing.assert_equal(result_array, a.compute())
    assert result.metadata == metadata
    assert result.specs == specs
    assert result.references == references


def test_write_dataframe_partitioned(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
    )

    data = {f"Column{i}": (1 + i) * numpy.ones(10) for i in range(5)}
    df = pandas.DataFrame(data)
    ddf = dask.dataframe.from_pandas(df, npartitions=3)
    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    references = [{"label": "test", "url": "http://www.test.com"}]

    client.write_dataframe(ddf, metadata=metadata, specs=specs, references=references)

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_dataframe = result.read()

    pandas.testing.assert_frame_equal(result_dataframe, df)
    assert result.metadata == metadata
    # TODO In the future this will be accessible via result.specs.
    assert result.specs == specs
    assert result.references == references


def test_write_sparse_full(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
    )

    coo = sparse.COO(coords=[[0, 1], [2, 3]], data=[3.8, 4.0], shape=(4, 4))

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    references = [{"label": "test", "url": "http://www.test.com"}]
    client.write_sparse(
        coords=coo.coords,
        data=coo.data,
        shape=coo.shape,
        metadata=metadata,
        specs=specs,
        references=references,
    )

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_array = result.read()

    numpy.testing.assert_equal(result_array.todense(), coo.todense())
    assert result.metadata == metadata
    assert result.specs == specs
    assert result.references == references


def test_write_sparse_chunked(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
    )

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    references = [{"label": "test", "url": "http://www.test.com"}]
    N = 5
    x = client.new(
        "sparse",
        COOStructure(shape=(2 * N,), chunks=((N, N),)),
        metadata=metadata,
        specs=specs,
        references=references,
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
    assert result.references == references


def test_update_array_metadata(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
    )

    # Update metadata in array client
    test_array = numpy.ones((5, 5))

    x = client.write_array(
        test_array, metadata={"scan_id": 1, "method": "A"}, specs=["SomeSpec"]
    )

    new_arr_metadata = {"scan_id": 2, "method": "A"}
    new_spec = ["AnotherSpec"]
    references = [{"label": "test", "url": "http://www.test.com"}]
    x.update_metadata(new_arr_metadata, new_spec, references)

    # validate local data after update request
    assert x.metadata == new_arr_metadata
    assert x.specs == new_spec
    assert x.references == references

    # Update metadata again to create another entry in revisions
    newer_arr_metadata = {"scan_id": 2, "method": "B"}
    newer_spec = ["AnotherOtherSpec"]
    new_references = [{"label": "updated_test", "url": "http://www.updatedtest.com"}]
    x.update_metadata(newer_arr_metadata, newer_spec, new_references)

    # Increase the size of revisions for additonal testing
    latest_arr_metadata = {"scan_id": 2, "method": "C"}
    x.update_metadata(latest_arr_metadata)

    results = client.search(Key("scan_id") == 2)
    result = results.values().first()

    # validate remote data after update request
    assert result.metadata == latest_arr_metadata
    assert result.specs == newer_spec
    assert result.references == new_references

    rev_document = {
        "key": result.item["id"],
        "revision": result.metadata_revisions[0]["revision"],
    }
    rev_document.update(result.metadata_revisions[0]["attributes"])
    assert DocumentRevision.from_json(rev_document)

    assert len(result.metadata_revisions[0:2]) == 2
    assert len(result.metadata_revisions) == len(result.metadata_revisions[:])

    result.metadata_revisions.delete_revision(0)
    assert len(result.metadata_revisions[:]) == 2


def test_update_dataframe_metadata(tmpdir):

    tree = MongoAdapter.from_mongomock(tmpdir)

    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY},
        validation_registry=validation_registry,
    )

    test_array = numpy.ones((5, 5))

    # Update metadata in dataframe client
    data = {
        "Column1": test_array[0],
        "Column2": test_array[1],
        "Column3": test_array[2],
        "Column4": test_array[3],
        "Column5": test_array[4],
    }

    test_dataframe = pandas.DataFrame(data)

    y = client.write_dataframe(
        test_dataframe, metadata={"scan_id": 1, "method": "A"}, specs=["SomeSpec"]
    )

    new_df_metadata = {"scan_id": 2, "method": "A"}
    new_spec = ["AnotherSpec"]
    references = [{"label": "test", "url": "http://www.test.com"}]
    y.update_metadata(new_df_metadata, new_spec, references)

    # validate local data after update request
    assert y.metadata == new_df_metadata
    assert y.specs == new_spec
    assert y.references == references

    # Update metadata again to create another entry in revisions
    newer_df_metadata = {"scan_id": 2, "method": "B"}
    newer_spec = ["AnotherOtherSpec"]
    new_references = [{"label": "updated_test", "url": "http://www.updatedtest.com"}]
    y.update_metadata(newer_df_metadata, newer_spec, new_references)

    # Increase the size of revisions for additonal testing
    latest_arr_metadata = {"scan_id": 2, "method": "C"}
    y.update_metadata(latest_arr_metadata)

    results = client.search(Key("scan_id") == 2)
    result = results.values().first()

    # validate remote data after update request
    assert result.metadata == latest_arr_metadata
    assert result.specs == newer_spec
    assert result.references == new_references

    rev_document = {
        "key": result.item["id"],
        "revision": result.metadata_revisions[0]["revision"],
    }
    rev_document.update(result.metadata_revisions[0]["attributes"])
    assert DocumentRevision.from_json(rev_document)

    assert len(result.metadata_revisions[0:2]) == 2
    assert len(result.metadata_revisions) == len(result.metadata_revisions[:])

    result.metadata_revisions.delete_revision(0)
    assert len(result.metadata_revisions[:]) == 2
