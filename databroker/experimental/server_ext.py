import builtins
import collections.abc
import itertools
import os
import shutil
import uuid
import time
from pathlib import Path
from datetime import datetime, timezone

import dask.dataframe
import numpy
import pymongo
import sparse
import zarr
import zarr.storage

from tiled.adapters.array import slice_and_shape_from_block_and_chunks
from tiled.adapters.array import ArrayAdapter
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.adapters.utils import IndexersMixin, tree_repr
from tiled.iterviews import KeysView, ItemsView, ValuesView
from tiled.queries import (
    Contains,
    Comparison,
    Eq,
    FullText,
    In,
    NotEq,
    NotIn,
    Operator,
    Regex,
)
from tiled.query_registration import QueryTranslationRegistry

from tiled.structures.core import StructureFamily
from tiled.structures.array import ArrayStructure, ArrayMacroStructure, BuiltinDtype
from tiled.structures.dataframe import (
    DataFrameStructure,
    DataFrameMacroStructure,
    DataFrameMicroStructure,
)
from tiled.structures.sparse import COOStructure
from tiled.serialization.dataframe import deserialize_arrow

from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE, UNCHANGED, OneShotCachedMap

from .schemas import Document, DocumentRevision

from sys import modules, platform


class Revisions:
    def __init__(self, collection, key):
        self._collection = collection
        self._key = key

    def __len__(self):
        return self._collection.count_documents({"key": self._key})

    def __getitem__(self, item_):
        offset = item_.start
        limit = item_.stop - offset
        return list(
            self._collection.find({"key": self._key})
            .sort("revision", 1)
            .skip(offset)
            .limit(limit)
        )

    def delete_revision(self, n):
        self._collection.delete_one({"key": self._key, "revision": n})

    def delete_all(self):
        self._collection.delete_many({"key": self._key})

    def last(self):
        return self._collection.find_one({"key": self._key}, sort=[("revision", -1)])

    def add_document(self, document):
        return self._collection.insert_one(document.dict())


class WritingArrayAdapter:
    structure_family = "array"

    def __init__(self, database, key):
        self.collection = database["nodes"]
        self.revisions = Revisions(database["revisions"], key)
        self.key = key
        self.deadline = 0
        assert self.doc.data_blob is None  # not implemented
        self.array = zarr.open_array(str(safe_path(self.doc.data_url.path)), "r+")

    @classmethod
    def new(cls, database, doc):
        # Zarr requires evently-sized chunks within each dimension.
        # Use the first chunk along each dimension.
        chunks = tuple(dim[0] for dim in doc.structure.macro.chunks)
        shape = tuple(dim[0] * len(dim) for dim in doc.structure.macro.chunks)
        storage = zarr.storage.DirectoryStore(str(safe_path(doc.data_url.path)))
        zarr.storage.init_array(
            storage,
            shape=shape,
            chunks=chunks,
            dtype=doc.structure.micro.to_numpy_dtype(),
        )
        return cls(database, doc.key)

    @property
    def doc(self):
        now = time.monotonic()
        if now > self.deadline:
            self._doc = Document(**self.collection.find_one({"key": self.key}))
            self.deadline = now + 0.1  # In seconds

        return self._doc

    @property
    def structure(self):
        # Convert pydantic implementation to dataclass implemenetation
        # expected by server.
        return ArrayStructure(**self.doc.structure.dict())

    @property
    def metadata(self):
        return self.doc.metadata

    @property
    def specs(self):
        return self.doc.specs

    @property
    def references(self):
        return self.doc.references

    def read(self, slice=None):
        # Trim overflow because Zarr always has equal-sized chunks.
        arr = self.array[
            tuple(builtins.slice(0, dim) for dim in self.doc.structure.macro.shape)
        ]
        if slice is not None:
            arr = arr[slice]
        return arr

    def read_block(self, block, slice=None):
        # Trim overflow because Zarr always has equal-sized chunks.
        slice_, _ = slice_and_shape_from_block_and_chunks(
            block, self.doc.structure.macro.chunks
        )
        # Slice the block out of the whole array.
        arr = self.array[slice_]
        # And then maybe slice *within* the block.
        if slice is not None:
            arr = arr[slice]
        return arr

    def microstructure(self):
        return BuiltinDtype(**self.doc.structure.micro.dict())

    def macrostructure(self):
        return ArrayMacroStructure(**self.doc.structure.macro.dict())

    def put_data(self, body, block=None):
        # Organize files into subdirectories with the first two
        # characters of the key to avoid one giant directory.
        if block:
            slice_, shape = slice_and_shape_from_block_and_chunks(
                block, self.doc.structure.macro.chunks
            )
        else:
            slice_ = numpy.s_[:]
            shape = self.doc.structure.macro.shape
        array = numpy.frombuffer(
            body, dtype=self.doc.structure.micro.to_numpy_dtype()
        ).reshape(shape)
        self.array[slice_] = array

    def put_metadata(self, metadata, specs, references):
        last_revision_doc = self.revisions.last()
        if last_revision_doc is not None:
            revision = int(last_revision_doc["revision"]) + 1
        else:
            revision = 0

        validated_revision = DocumentRevision.from_document(self.doc, revision)

        self.revisions.add_document(validated_revision)
        updated_at = datetime.now(tz=timezone.utc)

        to_set = {"updated_at": updated_at}

        if metadata is not None:
            to_set["metadata"] = metadata

        if specs is not None:
            to_set["specs"] = specs

        if references is not None:
            references_dict = [item.dict() for item in references]
            to_set["references"] = references_dict

        result = self.collection.update_one(
            {"key": self.key},
            {"$set": to_set},
        )

        if result.matched_count != result.modified_count:
            raise RuntimeError("Error while writing to database")

    def delete(self):
        shutil.rmtree(safe_path(self.doc.data_url.path))
        result = self.collection.delete_one({"key": self.key})
        assert result.deleted_count == 1
        self.revisions.delete_all()


class WritingDataFrameAdapter:
    structure_family = "dataframe"

    def __init__(self, database, key):
        self.collection = database["nodes"]
        self.revisions = Revisions(database["revisions"], key)
        self.key = key
        self.deadline = 0
        assert self.doc.data_blob is None  # not implemented

    @property
    def dataframe_adapter(self):
        return DataFrameAdapter.from_dask_dataframe(
            dask.dataframe.read_parquet(safe_path(self.doc.data_url.path))
        )

    @classmethod
    def new(cls, database, doc):
        safe_path(doc.data_url.path).mkdir(parents=True)
        return cls(database, doc.key)

    @property
    def doc(self):
        now = time.monotonic()
        if now > self.deadline:
            self._doc = Document(
                **self.collection.find_one({"key": self.key})
            )  # run query
            self.deadline = now + 0.1  # In seconds

        return self._doc

    @property
    def structure(self):
        # Convert pydantic implementation to dataclass implemenetation
        # expected by server.
        return DataFrameStructure(**self.doc.structure.dict())

    @property
    def metadata(self):
        return self.doc.metadata

    @property
    def specs(self):
        return self.doc.specs

    @property
    def references(self):
        return self.doc.references

    def __getitem__(self, key):
        return ArrayAdapter(self.dataframe_adapter.read([key])[key].values)

    def read(self, *args, **kwargs):
        return self.dataframe_adapter.read(*args, **kwargs)

    def read_partition(self, *args, **kwargs):
        return self.dataframe_adapter.read_partition(*args, **kwargs)

    def microstructure(self):
        return DataFrameMicroStructure(**self.doc.structure.micro.dict())

    def macrostructure(self):
        return DataFrameMacroStructure(**self.doc.structure.macro.dict())

    def put_data(self, body, partition=0):
        dataframe = deserialize_arrow(body)
        dataframe.to_parquet(
            safe_path(self.doc.data_url.path) / f"partition-{partition}.parquet"
        )

    def put_metadata(self, metadata, specs, references):
        last_revision_doc = self.revisions.last()
        if last_revision_doc is not None:
            revision = int(last_revision_doc["revision"]) + 1
        else:
            revision = 0

        validated_revision = DocumentRevision.from_document(self.doc, revision)

        result = self.revisions.add_document(validated_revision)
        updated_at = datetime.now(tz=timezone.utc)

        to_set = {"updated_at": updated_at}

        if metadata is not None:
            to_set["metadata"] = metadata

        if specs is not None:
            to_set["specs"] = specs

        if references is not None:
            references_dict = [item.dict() for item in references]
            to_set["references"] = references_dict

        result = self.collection.update_one(
            {"key": self.key},
            {"$set": to_set},
        )

        if result.matched_count != result.modified_count:
            raise RuntimeError("Error while writing to database")

    def delete(self):
        shutil.rmtree(safe_path(self.doc.data_url.path))
        result = self.collection.delete_one({"key": self.doc.key})
        assert result.deleted_count == 1
        self.revisions.delete_all()


class WritingCOOAdapter:
    structure_family = "sparse"

    def __init__(self, database, key):
        def load(filepath):
            import pandas

            df = pandas.read_parquet(filepath)
            coords = df[df.columns[:-1]].values.T
            data = df["data"].values
            return coords, data

        self.collection = database["nodes"]
        self.revisions = Revisions(database["revisions"], key)
        self.key = key
        self.deadline = 0
        assert self.doc.data_blob is None  # not implemented
        num_blocks = (range(len(n)) for n in self.doc.structure.chunks)
        directory = safe_path(self.doc.data_url.path)
        mapping = {}
        for block in itertools.product(*num_blocks):
            filepath = directory / f"block-{'.'.join(map(str, block))}.parquet"
            if filepath.is_file():
                mapping[block] = lambda filepath=filepath: load(filepath)
        self.blocks = OneShotCachedMap(mapping)

    @property
    def doc(self):
        now = time.monotonic()
        if now > self.deadline:
            self._doc = Document(
                **self.collection.find_one({"key": self.key})
            )  # run query
            self.deadline = now + 0.1  # In seconds

        return self._doc

    @classmethod
    def new(cls, database, doc):
        safe_path(doc.data_url.path).mkdir(parents=True)
        return cls(database, doc.key)

    @property
    def metadata(self):
        return self.doc.metadata

    @property
    def specs(self):
        return self.doc.specs

    @property
    def references(self):
        return self.doc.references

    def read_block(self, block, slice=None):
        coords, data = self.blocks[block]
        _, shape = slice_and_shape_from_block_and_chunks(
            block, self.doc.structure.chunks
        )
        arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
        if slice:
            arr = arr[slice]
        return arr

    def read(self, slice=None):
        all_coords = []
        all_data = []
        for (block, (coords, data)) in self.blocks.items():
            offsets = []
            for b, c in zip(block, self.doc.structure.chunks):
                offset = sum(c[:b])
                offsets.append(offset)
            global_coords = coords + [[i] for i in offsets]
            all_coords.append(global_coords)
            all_data.append(data)
        arr = sparse.COO(
            data=numpy.concatenate(all_data),
            coords=numpy.concatenate(all_coords, axis=-1),
            shape=self.doc.structure.shape,
        )
        if slice:
            return arr[slice]
        return arr

    def structure(self):
        # Convert pydantic implementation to dataclass implemenetation
        # expected by server.
        return COOStructure(**self.doc.structure.dict())

    def put_data(self, body, block=None):
        if block is None:
            block = (0,) * len(self.doc.structure.shape)
        dataframe = deserialize_arrow(body)
        dataframe.to_parquet(
            safe_path(self.doc.data_url.path)
            / f"block-{'.'.join(map(str, block))}.parquet"
        )

    def put_metadata(self, metadata, specs, references):
        last_revision_doc = self.revisions.last()
        if last_revision_doc is not None:
            revision = int(last_revision_doc["revision"]) + 1
        else:
            revision = 0

        validated_revision = DocumentRevision.from_document(self.doc, revision)

        result = self.revisions.add_document(validated_revision)
        updated_at = datetime.now(tz=timezone.utc)

        to_set = {"updated_at": updated_at}

        if metadata is not None:
            to_set["metadata"] = metadata

        if specs is not None:
            to_set["specs"] = specs

        if references is not None:
            references_dict = [item.dict() for item in references]
            to_set["references"] = references_dict

        result = self.collection.update_one(
            {"key": self.key},
            {"$set": to_set},
        )

        if result.matched_count != result.modified_count:
            raise RuntimeError("Error while writing to database")

    def delete(self):
        shutil.rmtree(safe_path(self.doc.data_url.path))
        result = self.collection.delete_one({"key": self.doc.key})
        assert result.deleted_count == 1
        self.revisions.delete_all()


class MongoAdapter(collections.abc.Mapping, IndexersMixin):
    structure_family = "node"
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register

    def __init__(
        self,
        *,
        database,
        directory,
        queries=None,
        sorting=None,
        metadata=None,
        access_policy=None,
    ):
        self.database = database
        self.collection = database["nodes"]
        self.revision_coll = database["revisions"]
        self.directory = Path(directory).resolve()
        if not self.directory.exists():
            raise ValueError(f"Directory {self.directory} does not exist.")
        if not self.directory.is_dir():
            raise ValueError(
                f"The given directory path {self.directory} is not a directory."
            )
        if not os.access(self.directory, os.W_OK):
            raise ValueError(f"Directory {self.directory} is not writeable.")
        self.queries = queries or []
        self.sorting = sorting or [("metadata.scan_id", 1)]
        self.metadata = metadata or {}
        self.access_policy = access_policy
        super().__init__()

    @classmethod
    def from_uri(cls, uri, directory, *, metadata=None):
        """
        When calling this method, call create_index() from its instance to define the
        unique indexes in the revision collection

        """
        if not pymongo.uri_parser.parse_uri(uri)["database"]:
            raise ValueError(
                f"Invalid URI: {uri!r} Did you forget to include a database?"
            )
        client = pymongo.MongoClient(uri)
        database = client.get_database()
        return cls(database=database, directory=directory, metadata=metadata)

    @classmethod
    def from_mongomock(cls, directory, *, metadata=None):
        import mongomock

        db_name = f"temp-{str(uuid.uuid4())}"
        mongo_client = mongomock.MongoClient()
        database = mongo_client[db_name]

        mongo_adapter = cls(database=database, directory=directory, metadata=metadata)
        mongo_adapter.create_indexes()

        return mongo_adapter

    def new_variation(
        self,
        metadata=UNCHANGED,
        queries=UNCHANGED,
        sorting=UNCHANGED,
        **kwargs,
    ):
        if metadata is UNCHANGED:
            metadata = self.metadata
        if queries is UNCHANGED:
            queries = self.queries
        if sorting is UNCHANGED:
            sorting = self.sorting
        return type(self)(
            database=self.database,
            directory=self.directory,
            metadata=metadata,
            queries=queries,
            sorting=sorting,
            access_policy=self.access_policy,
            **kwargs,
        )

    def post_metadata(self, metadata, structure_family, structure, specs, references):

        mime_structure_association = {
            StructureFamily.array: "application/x-zarr",
            StructureFamily.dataframe: APACHE_ARROW_FILE_MIME_TYPE,
            StructureFamily.sparse: APACHE_ARROW_FILE_MIME_TYPE,
        }

        key = str(uuid.uuid4())
        created_date = datetime.now(tz=timezone.utc)

        validated_document = Document(
            key=key,
            structure_family=structure_family,
            structure=structure,
            metadata=metadata,
            specs=specs,
            references=references,
            mimetype=mime_structure_association[structure_family],
            data_url=f"file://localhost/{self.directory}/{key[:2]}/{key}",
            created_at=created_date,
            updated_at=created_date,
        )

        _adapter_class_by_family[structure_family]
        self.collection.insert_one(validated_document.dict())
        _adapter_class_by_family[structure_family].new(
            self.database, validated_document
        )
        return key

    def create_indexes(self):
        self.collection.create_index([("key", pymongo.ASCENDING)], unique=True)

        self.revision_coll.create_index(
            [("key", pymongo.ASCENDING), ("revision", pymongo.DESCENDING)], unique=True
        )

    def _build_mongo_query(self, *queries):
        combined = self.queries + list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def __getitem__(self, key):
        query = {"key": key}
        doc = self.collection.find_one(self._build_mongo_query(query), {"_id": False})
        if doc is None:
            raise KeyError(key)

        class_ = _adapter_class_by_family[StructureFamily(doc["structure_family"])]
        return class_(self.database, key)

    def __iter__(self):
        # TODO Apply pagination, as we do in Databroker.
        for doc in list(
            self.collection.find(
                self._build_mongo_query({"data_url": {"$ne": None}}),
                {"key": True},
            )
        ):
            yield doc["key"]

    def __len__(self):
        return self.collection.count_documents(
            self._build_mongo_query({"data_url": {"$ne": None}})
        )

    def __length_hint__(self):
        # https://www.python.org/dev/peps/pep-0424/
        return self.collection.estimated_document_count(
            self._build_mongo_query({"data_url": {"$ne": None}}),
        )

    def __repr__(self):
        # Display up to the first N keys to avoid making a giant service
        # request. Use _keys_slicer because it is unauthenticated.
        N = 10
        return tree_repr(self, self._keys_slice(0, N, direction=1))

    def search(self, query):
        """
        Return a MongoAdapter with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def sort(self, sorting):
        return self.new_variation(sorting=sorting)

    def keys(self):
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self):
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self):
        return ItemsView(lambda: len(self), self._items_slice)

    def _keys_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None
        for doc in self.collection.find(
            self._build_mongo_query({"data_url": {"$ne": None}}),
            skip=skip,
            limit=limit,
        ):
            yield doc["key"]

    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        for doc in self.collection.find(
            self._build_mongo_query({"data_url": {"$ne": None}}),
            skip=skip,
            limit=limit,
        ):
            class_ = _adapter_class_by_family[StructureFamily(doc["structure_family"])]
            yield doc["key"], class_(self.database, doc["key"])

    def apply_mongo_query(self, query):
        return self.new_variation(
            queries=self.queries + [query],
        )


def contains(query, catalog):
    # In MongoDB, checking that an item is in an array looks
    # just like equality.
    # https://www.mongodb.com/docs/manual/tutorial/query-arrays/
    return catalog.apply_mongo_query({f"metadata.{query.key}": query.value})


def comparison(query, catalog):
    OPERATORS = {
        Operator.lt: "$lt",
        Operator.le: "$lte",
        Operator.gt: "$gt",
        Operator.ge: "$gte",
    }
    return catalog.apply_mongo_query(
        {f"metadata.{query.key}": {OPERATORS[query.operator]: query.value}}
    )


def eq(query, catalog):
    return catalog.apply_mongo_query({f"metadata.{query.key}": query.value})


def noteq(query, catalog):
    return catalog.apply_mongo_query({f"metadata.{query.key}": {"$ne": query.value}})


def regex(query, catalog):
    options = "" if query.case_sensitive else "i"
    return catalog.apply_mongo_query(
        {f"metadata.{query.key}": {"$regex": query.pattern, "$options": options}}
    )


def _in(query, catalog):
    if not isinstance(query.value, list):
        query.value = [query.value]
    return catalog.apply_mongo_query({f"metadata.{query.key}": {"$in": query.value}})


def notin(query, catalog):
    if not isinstance(query.value, list):
        query.value = [query.value]
    return catalog.apply_mongo_query({f"metadata.{query.key}": {"$nin": query.value}})


def full_text_search(query, catalog):
    # First if this catalog is backed by mongomock, which does not support $text queries.
    # Avoid importing mongomock if it is not already imported.
    if "mongomock" in modules:
        import mongomock

        if isinstance(catalog.database.client, mongomock.MongoClient):
            # Do the query in memory.
            # For huge MongoAdapters this will be slow, but if you are attempting
            # full text search on a large mongomock-backed MongoAdapter,
            # you have made your choices! :-)
            return MapAdapter(dict(catalog)).search(query)

    return catalog.apply_mongo_query(
        {"$text": {"$search": query.text, "$caseSensitive": query.case_sensitive}},
    )


MongoAdapter.register_query(Contains, contains)
MongoAdapter.register_query(Comparison, comparison)
MongoAdapter.register_query(Eq, eq)
MongoAdapter.register_query(NotEq, noteq)
MongoAdapter.register_query(Regex, regex)
MongoAdapter.register_query(In, _in)
MongoAdapter.register_query(NotIn, notin)
MongoAdapter.register_query(FullText, full_text_search)


def safe_path(path):
    if platform == "win32" and path[0] == "/":
        path = path[1:]
    return Path(path)


_adapter_class_by_family = {
    StructureFamily.array: WritingArrayAdapter,
    StructureFamily.dataframe: WritingDataFrameAdapter,
    StructureFamily.sparse: WritingCOOAdapter,
}
