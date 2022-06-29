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

from ..experimental.server_ext import MongoAdapter

import numpy
import pandas
import string


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
