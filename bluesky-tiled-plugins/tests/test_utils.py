import orjson
from bluesky_tiled_plugins.utils import truncate_json_overflow


def test_truncate_json_overflow():
    # Test with a large integer
    data = {"large_pos_int": 2**60, "large_neg_int": -(2**60)}
    truncated_data = truncate_json_overflow(data)
    assert orjson.dumps(truncated_data, option=orjson.OPT_STRICT_INTEGER)
    for val in orjson.loads(orjson.dumps(truncated_data, option=orjson.OPT_STRICT_INTEGER)).values():
        assert val is not None

    # Test with a large float
    data = {"large_pos_float": 2e308, "large_neg_float": -2e308}
    truncated_data = truncate_json_overflow(data)
    assert orjson.dumps(truncated_data, option=orjson.OPT_STRICT_INTEGER)
    for val in orjson.loads(orjson.dumps(truncated_data, option=orjson.OPT_STRICT_INTEGER)).values():
        assert val is not None

    # Test with a list of large integers and floats
    data = [[2**60, -(2**60)], [2e308, -2e308]]
    truncated_data = truncate_json_overflow(data)
    assert orjson.dumps(truncated_data, option=orjson.OPT_STRICT_INTEGER)

    # Test with a dictionary containing various types
    data = {
        "int": 42,
        "float": 3.14,
        "str": "Hello, world!",
        "list": [1, 2, 3],
        "dict": {"key": "value"},
        "large_int": 2**60,
        "large_float": 2e308,
        "nested": {
            "large_neg_int": -(2**60),
            "large_neg_float": -2e308,
            "list_of_large_ints": [2**60, -(2**60)],
            "list_of_large_floats": [2e308, -2e308],
        },
    }
    truncated_data = truncate_json_overflow(data)
    assert orjson.dumps(truncated_data, option=orjson.OPT_STRICT_INTEGER)

    # Test with a NaN value
    data = {"nan": float("nan")}
    truncated_data = truncate_json_overflow(data)
    assert orjson.loads(orjson.dumps(truncated_data, option=orjson.OPT_STRICT_INTEGER))["nan"] is None
