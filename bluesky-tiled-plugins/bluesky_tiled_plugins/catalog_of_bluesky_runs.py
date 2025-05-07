import collections.abc
import copy
import functools
import numbers
import operator

from tiled.adapters.utils import IndexCallable
from tiled.client.container import Container
from tiled.client.utils import handle_error
from tiled.queries import Comparison, Eq, Like
from tiled.utils import safe_json_dump

from .bluesky_run import BlueskyRunV2, BlueskyRunV3
from .queries import RawMongo, ScanIDRange, TimeRange, _PartialUID, _ScanID


class CatalogOfBlueskyRuns(Container):
    """
    This adds some bluesky-specific conveniences to the standard client Container.

    >>> catalog.scan_id[1234]  # scan_id lookup
    >>> catalog.uid["9acjef"]  # (partial) uid lookup
    >>> catalog[1234]  # automatically do scan_id lookup for positive integer
    >>> catalog["9acjef"]  # automatically do (partial) uid lookup for string
    >>> catalog[-5]  # automatically do catalog.values()[-N] for negative integer
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scan_id = IndexCallable(self._lookup_by_scan_id)
        self.uid = IndexCallable(self._lookup_by_partial_uid)
        self._v1 = None

    def __repr__(self):
        # This is a copy/paste of the general-purpose implementation
        # tiled.adapters.utils.tree_repr
        # with some modifications to extract scan_id from the metadata.
        sample = self.items()[:10]
        # Use scan_id (int) if defined; otherwise fall back to uid.
        sample_reprs = [repr(value.metadata.get("start", {}).get("scan_id", key)) for key, value in sample]
        out = "<Catalog {"
        # Always show at least one.
        if sample_reprs:
            out += sample_reprs[0]
        # And then show as many more as we can fit on one line.
        counter = 1
        for sample_repr in sample_reprs[1:]:
            if len(out) + len(sample_repr) > 60:  # character count
                break
            out += ", " + sample_repr
            counter += 1
        approx_len = operator.length_hint(self)  # cheaper to compute than len(node)
        # Are there more in the node that what we displayed above?
        if approx_len > counter:
            out += f", ...}} ~{approx_len} entries>"
        else:
            out += "}>"
        return out

    @property
    def v1(self):
        "Accessor to legacy interface."
        if self._v1 is None:
            from databroker.v1 import Broker

            self._v1 = Broker(self)
            self._v1._version = "1.0"
        return self._v1

    @property
    def v2(self):
        structure_clients = copy.copy(self.structure_clients)
        structure_clients.set("BlueskyRun", lambda: BlueskyRunV2)
        return CatalogOfBlueskyRuns(self.context, item=self.item, structure_clients=structure_clients)

    @property
    def v3(self):
        if not self.is_sql:
            raise NotImplementedError("v3 is only available for SQL-based catalogs.")

        structure_clients = copy.copy(self.structure_clients)
        structure_clients.set("BlueskyRun", lambda: BlueskyRunV3)
        return CatalogOfBlueskyRuns(self.context, item=self.item, structure_clients=structure_clients)

    @functools.cached_property
    def is_sql(self):
        for spec in self.specs:
            if spec.name == "CatalogOfBlueskyRuns":
                if spec.version and spec.version.startswith("3."):
                    return True
                return False

    def __getitem__(self, key):
        # For convenience and backward-compatiblity reasons, we support
        # some "magic" here that is helpful in an interactive setting.
        if isinstance(key, str):
            # CASE 1: Interpret key as a uid or partial uid.
            if len(key) == 36:
                # This looks like a full uid. Try direct lookup first.
                try:
                    return super().__getitem__(key)
                except KeyError:
                    # Fall back to partial uid lookup below.
                    pass
            return self._lookup_by_partial_uid(key)
        elif isinstance(key, numbers.Integral):
            if key > 0:
                # CASE 2: Interpret key as a scan_id.
                return self._lookup_by_scan_id(int(key))
            else:
                # CASE 3: Interpret key as a recently lookup, as in
                # `catalog[-1]` is the latest entry.
                key = int(key)
                return self.values()[key]
        elif isinstance(key, slice):
            if (key.start is None) or (key.start >= 0):
                raise ValueError(
                    "For backward-compatibility reasons, slicing here "
                    "is limited to negative indexes. "
                    "Use .values() to slice how you please."
                )
            return self.values()[key]
        elif isinstance(key, collections.abc.Iterable):
            # We know that isn't a str because we check that above.
            # Recurse.
            return [self[item] for item in key]
        else:
            raise ValueError("Indexing expects a string, an integer, or a collection of strings and/or integers.")

    def _lookup_by_scan_id(self, scan_id):
        results = self.search(Eq("start.scan_id", scan_id))
        if not results:
            raise KeyError(f"No match for scan_id={scan_id}")
        else:
            # Return latest match.
            return results.values().last()

    def _lookup_by_partial_uid(self, partial_uid):
        if len(partial_uid) < 5:
            raise ValueError(f"Partial uid {partial_uid!r} is too short. It must include at least 5 characters.")
        if self.is_sql:
            query = Like("start.uid", f"{partial_uid}%")
        else:
            query = _PartialUID(partial_uids=[partial_uid])
        results = self.search(query).values().head(2)
        if len(results) > 1:
            raise ValueError(
                f"Partial uid {partial_uid} has multiple matches. Include more characters to get a unique match."
            )
        if not results:
            raise KeyError(f"No match for partial_uid {partial_uid}")
        # There is one unique result.
        return results[0]

    def get_serializer(self):
        from tiled.server.app import get_root_tree

        if not hasattr(self.context.http_client, "app"):
            raise NotImplementedError("Only works on local application.")
        tree = self.context.http_client.app.dependency_overrides[get_root_tree]()
        return tree.get_serializer()

    def search(self, query):
        # These query types were formerly handled server side by specially-registered
        # queries. Now that are transformed client side into generic queries that
        # come standard with the Tiled server.

        # Some need to be expressed as a chain of queries.
        if isinstance(query, TimeRange):
            result = self
            if query.since:
                result = Container.search(result, Comparison("ge", "start.time", query.since))
            if query.until:
                result = Container.search(result, Comparison("lt", "start.time", query.until))
        # For backward-compatiblity, accept a dict and interpret it as a Mongo
        # query against the 'start' documents.
        elif isinstance(query, _ScanID):
            if len(query.scan_ids) > 1:
                raise ValueError("Search on multiple ScanIDs in one query is no longer supported.")
            (scan_id,) = query.scan_ids
            query = Eq("start.scan_id", int(scan_id))
            result = super().search(query)
        elif isinstance(query, _PartialUID):
            if len(query.partial_uids) > 1:
                raise ValueError("Search on multiple PartialUIDs in one query is no longer supported.")
            (partial_uid,) = query.partial_uids
            if self.is_sql:
                query = Like("start.uid", f"{partial_uid}%")
            else:
                query = _PartialUID(partial_uids=[partial_uid])
            result = super().search(query)
        elif isinstance(query, ScanIDRange):
            ge = Comparison("ge", "start.scan_id", query.start_id)
            lt = Comparison("lt", "start.scan_id", query.end_id)
            result = super().search(ge).search(lt)
        elif isinstance(query, dict):
            query = RawMongo(start=query)
            result = super().search(query)
        else:
            if hasattr(query, "key"):
                if not query.key.startswith("start.") or query.key.startswith("stop."):
                    # Default to searching RunStart document.
                    query = copy.copy(query)
                    query.key = f"start.{query.key}"
            result = super().search(query)
        return result

    def post_document(self, name, doc):
        link = self.item["links"]["self"].replace("/metadata", "/documents", 1)
        response = self.context.http_client.post(link, content=safe_json_dump({"name": name, "doc": doc}))
        handle_error(response)
