import collections

from tiled.adapters.mapping import MapAdapter, full_text_search
from tiled.queries import (
    Contains,
    Comparison,
    Eq,
    FullText,
    In,
    NotEq,
    NotIn,
    Operator,
    QueryValueError,
    Regex,
)
from bluesky_tiled_plugins.queries import (
    _PartialUID,
    _ScanID,
    ScanIDRange,
    TimeRange,
)
from tiled.query_registration import QueryTranslationRegistry


class BlueskyMapAdapter(MapAdapter):
    """
    A Tree that contains BlueskyRuns and supports relevant queries on them.
    """

    # The primary purpose of this class is to have a query_registry
    # distinct form the generic tiled.in_memory.Tree.query_registry
    # with queries that assume the contents are BlueskyRuns and have the
    # requisite metadata structure.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    def apply_mongo_query(self, query):

        from mongoquery import Query

        query_obj = Query(query)
        matches = {
            key: value
            for key, value in self.items()
            if query_obj.match(value.metadata()["start"])
        }
        return type(self)(mapping=matches)


def scan_id(query, catalog):
    mongo_results = catalog.apply_mongo_query({"scan_id": {"$in": query.scan_ids}})
    # Handle duplicates.
    if query.duplicates == "latest":
        # Convert to a BlueskyMapAdapter to do some filtering in Python
        # that we cannot expressing in a collection.find(...) query.
        # We might want to rethink this later and make it possible to do
        # aggregations in Mongo from queries.
        results_by_scan_id = {}
        for key, value in mongo_results.items():
            results_by_scan_id[value.metadata()["start"]["scan_id"]] = (key, value)
        results = BlueskyMapAdapter(
            dict(results_by_scan_id.values()), must_revalidate=False
        )
    elif query.duplicates == "error":
        scan_ids = list(
            value.metadata()["start"]["scan_id"] for value in mongo_results.values()
        )
        counter = collections.Counter(scan_ids)
        duplicated = []
        for k, v in counter.items():
            if v > 1:
                duplicated.append(k)
        if duplicated:
            raise QueryValueError(
                f"There are multiples of the following scan_ids: {duplicated}"
            )
        results = mongo_results
    elif query.duplicates == "all":
        results = mongo_results
    else:
        raise QueryValueError("duplicates should be one of {'latest', 'error', 'all'}")
    return results


def scan_id_range(query, catalog):
    mongo_results = catalog.apply_mongo_query(
        {"scan_id": {"$gte": query.start_id, "$lt": query.end_id}}
    )
    # Handle duplicates.
    if query.duplicates == "latest":
        # Convert to a BlueskyMapAdapter to do some filtering in Python
        # that we cannot expressing in a collection.find(...) query.
        # We might want to rethink this later and make it possible to do
        # aggregations in Mongo from queries.
        results_by_scan_id = {}
        for key, value in mongo_results.items():
            results_by_scan_id[value.metadata()["start"]["scan_id"]] = (key, value)
        results = BlueskyMapAdapter(
            dict(results_by_scan_id.values()), must_revalidate=False
        )
    elif query.duplicates == "error":
        scan_ids = list(
            value.metadata()["start"]["scan_id"] for value in mongo_results.values()
        )
        counter = collections.Counter(scan_ids)
        duplicated = []
        for k, v in counter.items():
            if v > 1:
                duplicated.append(k)
        if duplicated:
            raise QueryValueError(
                f"There are multiples of the following scan_ids: {duplicated}"
            )
        results = mongo_results
    elif query.duplicates == "all":
        results = mongo_results
    else:
        raise QueryValueError("duplicates should be one of {'latest', 'error', 'all'}")
    return results


def partial_uid(query, catalog):
    results = {}
    for partial_uid in query.partial_uids:
        if len(partial_uid) < 5:
            raise QueryValueError(
                f"Partial uid {partial_uid!r} is too short. "
                "It must include at least 5 characters."
            )
        result = catalog.apply_mongo_query({"uid": {"$regex": f"^{partial_uid}"}})
        if len(result) > 1:
            raise QueryValueError(
                f"Partial uid {partial_uid} has multiple matches, "
                "listed below. Include more characters. Matches:\n" + "\n".join(result)
            )
        results.update(result)
    return BlueskyMapAdapter(results, must_revalidate=False)


def time_range(query, catalog):
    mongo_query = {"time": {}}
    if query.since is not None:
        mongo_query["time"]["$gte"] = query.since
    if query.until is not None:
        mongo_query["time"]["$lt"] = query.until
    if not mongo_query["time"]:
        # Neither 'since' nor 'until' are set.
        mongo_query.clear()
    return catalog.apply_mongo_query(mongo_query)


def eq(query, catalog):
    key = query.key.removeprefix("start.")
    return catalog.apply_mongo_query({key: query.value})


def contains(query, catalog):
    # In MongoDB, checking that an item is in an array looks
    # just like equality.
    # https://www.mongodb.com/docs/manual/tutorial/query-arrays/
    key = query.key.removeprefix("start.")
    return catalog.apply_mongo_query({key: query.value})


def _in(query, catalog):
    key = query.key.removeprefix("start.")
    return catalog.apply_mongo_query({key: {"$in": query.value}})


def not_in(query, catalog):
    key = query.key.removeprefix("start.")
    return catalog.apply_mongo_query({key: {"$nin": query.value}})


def not_eq(query, catalog):
    key = query.key.removeprefix("start.")
    return catalog.apply_mongo_query({key: {"$ne": query.value}})


def comparison(query, catalog):
    OPERATORS = {
        Operator.lt: "$lt",
        Operator.le: "$lte",
        Operator.gt: "$gt",
        Operator.ge: "$gte",
    }
    key = query.key.removeprefix("start.")
    return catalog.apply_mongo_query(
        {key: {OPERATORS[query.operator]: query.value}}
    )


def regex(query, catalog):
    options = "" if query.case_sensitive else "i"
    key = query.key.removeprefix("start.")
    return catalog.apply_mongo_query(
        {key: {"$regex": query.pattern, "$options": options}}
    )


BlueskyMapAdapter.register_query(_PartialUID, partial_uid)
BlueskyMapAdapter.register_query(_ScanID, scan_id)
BlueskyMapAdapter.register_query(ScanIDRange, scan_id)
BlueskyMapAdapter.register_query(FullText, full_text_search)
BlueskyMapAdapter.register_query(Contains, contains)
BlueskyMapAdapter.register_query(Comparison, comparison)
BlueskyMapAdapter.register_query(Eq, eq)
BlueskyMapAdapter.register_query(FullText, full_text_search)
BlueskyMapAdapter.register_query(In, _in)
BlueskyMapAdapter.register_query(NotEq, not_eq)
BlueskyMapAdapter.register_query(NotIn, not_in)
BlueskyMapAdapter.register_query(TimeRange, time_range)
BlueskyMapAdapter.register_query(Regex, regex)
