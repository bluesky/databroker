import builtins
import collections.abc
import itertools
import os
import shutil
import uuid
from pathlib import Path

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
from tiled.structures.array import ArrayMacroStructure, BuiltinDtype
from tiled.structures.dataframe import DataFrameMacroStructure, DataFrameMicroStructure
from tiled.structures.sparse import COOStructure
from tiled.serialization.dataframe import deserialize_arrow

from tiled.server.pydantic_array import ArrayStructure
from tiled.server.pydantic_dataframe import DataFrameStructure
from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE, UNCHANGED, OneShotCachedMap

from .schemas import Document

from sys import modules, platform


class WritingArrayAdapter:
    structure_family = "array"

    def __init__(self, collection, doc):
        self.collection = collection
        self.doc = doc
        assert self.doc.data_blob is None  # not implemented
        self.array = zarr.open_array(str(safe_path(self.doc.data_url.path)), "r+")

    @classmethod
    def new(cls, collection, doc):
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
        return cls(collection, doc)

    @property
    def structure(self):
        return ArrayStructure.from_json(self.doc.structure)

    @property
    def metadata(self):
        return self.doc.metadata

    @property
    def specs(self):
        return self.doc.specs

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

    def delete(self):
        shutil.rmtree(safe_path(self.doc.data_url.path))
        result = self.collection.delete_one({"key": self.doc.key})
        assert result.deleted_count == 1


class WritingDataFrameAdapter:
    structure_family = "dataframe"

    def __init__(self, collection, doc):
        self.collection = collection
        self.doc = doc
        assert self.doc.data_blob is None  # not implemented

    @property
    def dataframe_adapter(self):
        return DataFrameAdapter.from_dask_dataframe(
            dask.dataframe.read_parquet(safe_path(self.doc.data_url.path))
        )

    @classmethod
    def new(cls, collection, doc):
        safe_path(doc.data_url.path).mkdir(parents=True)
        return cls(collection, doc)

    @property
    def structure(self):
        return DataFrameStructure.from_json(self.doc.structure)

    @property
    def metadata(self):
        return self.doc.metadata

    @property
    def specs(self):
        return self.doc.specs

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

    def delete(self):
        shutil.rmtree(safe_path(self.doc.data_url.path))
        result = self.collection.delete_one({"key": self.doc.key})
        assert result.deleted_count == 1


class WritingCOOAdapter:
    structure_family = "sparse"

    def __init__(self, collection, doc):
        def load(filepath):
            import pandas

            df = pandas.read_parquet(filepath)
            coords = df[df.columns[:-1]].values.T
            data = df["data"].values
            return coords, data

        self.collection = collection
        self.doc = doc
        assert self.doc.data_blob is None  # not implemented
        num_blocks = (range(len(n)) for n in self.doc.structure.chunks)
        directory = safe_path(self.doc.data_url.path)
        mapping = {}
        for block in itertools.product(*num_blocks):
            filepath = directory / f"block-{'.'.join(map(str, block))}.parquet"
            if filepath.is_file():
                mapping[block] = lambda filepath=filepath: load(filepath)
        self.blocks = OneShotCachedMap(mapping)

    @classmethod
    def new(cls, collection, doc):
        safe_path(doc.data_url.path).mkdir(parents=True)
        return cls(collection, doc)

    @property
    def metadata(self):
        return self.doc.metadata

    @property
    def specs(self):
        return self.doc.specs

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
        return COOStructure(**self.doc.structure.dict())

    def put_data(self, body, block=None):
        if block is None:
            block = (0,) * len(self.doc.structure.shape)
        dataframe = deserialize_arrow(body)
        dataframe.to_parquet(
            safe_path(self.doc.data_url.path)
            / f"block-{'.'.join(map(str, block))}.parquet"
        )

    def delete(self):
        shutil.rmtree(safe_path(self.doc.data_url.path))
        result = self.collection.delete_one({"key": self.doc.key})
        assert result.deleted_count == 1


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
        principal=None,
        access_policy=None,
    ):
        self.database = database
        self.collection = database["nodes"]
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
        self.principal = principal
        self.access_policy = access_policy
        super().__init__()

    @classmethod
    def from_uri(cls, uri, directory, *, metadata=None):
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

        return cls(database=database, directory=directory, metadata=metadata)

    def new_variation(
        self,
        metadata=UNCHANGED,
        queries=UNCHANGED,
        sorting=UNCHANGED,
        principal=UNCHANGED,
        **kwargs,
    ):
        if metadata is UNCHANGED:
            metadata = self.metadata
        if queries is UNCHANGED:
            queries = self.queries
        if sorting is UNCHANGED:
            sorting = self.sorting
        if principal is UNCHANGED:
            principal = self.principal
        return type(self)(
            database=self.database,
            directory=self.directory,
            metadata=metadata,
            queries=queries,
            sorting=sorting,
            access_policy=self.access_policy,
            principal=principal,
            **kwargs,
        )

    def post_metadata(self, metadata, structure_family, structure, specs):

        mime_structure_association = {
            StructureFamily.array: "application/x-zarr",
            StructureFamily.dataframe: APACHE_ARROW_FILE_MIME_TYPE,
            StructureFamily.sparse: APACHE_ARROW_FILE_MIME_TYPE,
        }

        key = str(uuid.uuid4())

        validated_document = Document(
            key=key,
            structure_family=structure_family,
            structure=structure,
            metadata=metadata,
            specs=specs,
            mimetype=mime_structure_association[structure_family],
            data_url=f"file://localhost/{self.directory}/{key[:2]}/{key}",
        )

        _adapter_class_by_family[structure_family]
        self.collection.insert_one(validated_document.dict())
        _adapter_class_by_family[structure_family].new(
            self.collection, validated_document
        )
        return key

    def authenticated_as(self, identity):
        if self.principal is not None:
            raise RuntimeError(f"Already authenticated as {self.principal}")
        if self.access_policy is not None:
            raise NotImplementedError("No support for Access Policy")
        return self

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
        return class_(self.collection, Document(**doc))

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
            yield doc["key"], class_(self.collection, Document(**doc))

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
