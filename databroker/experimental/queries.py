import collections.abc
from dataclasses import dataclass
import json

from tiled.queries import register


@register(name="raw_mongo")
@dataclass
class RawMongo:
    """
    Run a MongoDB query against a given collection.
    """

    query: str  # We cannot put a dict in a URL, so this a JSON str.

    def __init__(self, query):
        if not isinstance(query, str):
            query = json.dumps(query)
        self.query = query


def scan_id(number):
    return RawMongo({"metadata.scan_id": int(number)})
