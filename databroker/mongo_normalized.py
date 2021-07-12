import builtins
import collections
import collections.abc
import copy
import functools
import itertools
import json
import os
import sys
import warnings

from bson.objectid import ObjectId, InvalidId
import cachetools
import entrypoints
import event_model
from dask.array.core import cached_cumsum, normalize_chunks
import numpy
import pymongo
import toolz.itertoolz
import xarray

from tiled.structures.array import (
    ArrayStructure,
    ArrayMacroStructure,
    Kind,
    MachineDataType,
)
from tiled.structures.xarray import (
    DataArrayStructure,
    DataArrayMacroStructure,
    DatasetMacroStructure,
    VariableStructure,
    VariableMacroStructure,
)
from tiled.query_registration import QueryTranslationRegistry
from tiled.queries import FullText, KeyLookup
from tiled.utils import (
    DictView,
    SpecialUsers,
)
from tiled.trees.utils import (
    tree_repr,
    IndexersMixin,
    UNCHANGED,
)
from tiled.utils import import_object, OneShotCachedMap

from .common import BlueskyEventStreamMixin, BlueskyRunMixin, CatalogOfBlueskyRunsMixin
from .queries import (
    TreeInMemory,
    RawMongo,
    _PartialUID,
    _ScanID,
    TimeRange,
    key_lookup,
    partial_uid,
    scan_id,
    time_range,
)
from .server import router


CHUNK_SIZE_LIMIT = os.getenv("DATABROKER_CHUNK_SIZE_LIMIT", "100MB")


class BlueskyRun(TreeInMemory, BlueskyRunMixin):
    specs = ["BlueskyRun"]

    def __init__(
        self,
        *args,
        handler_registry,
        transforms,
        root_map,
        datum_collection,
        resource_collection,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.transforms = transforms or {}
        self.root_map = root_map
        self._datum_collection = datum_collection
        self._resource_collection = resource_collection
        # This is used to create the Filler on first access.
        self._init_handler_registry = handler_registry
        self._filler = None

    @property
    def metadata(self):
        "Metadata about this Tree."
        # If there are transforms configured, shadow the 'start' and 'stop' documents
        # with transfomed copies.
        transformed = {}
        if "start" in self.transforms:
            transformed["start"] = self.transforms["start"](self._metadata["start"])
        if "stop" in self.transforms:
            transformed["stop"] = self.transforms["stop"](self._metadata["stop"])
        metadata = collections.ChainMap(transformed, self._metadata)
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(metadata)

    @property
    def filler(self):
        if self._filler is None:
            self._filler = event_model.Filler(
                handler_registry=self._init_handler_registry,
                root_map=self.root_map,
                inplace=False,
            )
            for descriptor in itertools.chain(
                *(stream.metadata["descriptors"] for stream in self.values())
            ):
                self._filler("descriptor", descriptor)
        return self._filler

    @property
    def register_handler(self):
        return self.filler.register_handler

    @property
    def deregister_handler(self):
        return self.filler.deregister_handler

    @property
    def handler_registry(self):
        return self.filler.handler_registry

    def new_variation(self, *args, **kwargs):
        return super().new_variation(
            *args,
            handler_registry=self.handler_registry,
            transforms=self.transforms,
            root_map=self.root_map,
            datum_collection=self._datum_collection,
            resource_collection=self._resource_collection,
            **kwargs,
        )

    def get_datum_for_resource(self, resource_uid):
        return self._datum_collection.find({"resource": resource_uid}, {"_id": False})

    def get_resource(self, uid):
        doc = self._resource_collection.find_one({"uid": uid}, {"_id": False})

        # Some old resource documents don't have a 'uid' and they are
        # referenced by '_id'.
        if doc is None:
            try:
                _id = ObjectId(uid)
            except InvalidId:
                pass
            else:
                doc = self._resource_collection.find_one({"_id": _id}, {"_id": False})
                doc["uid"] = uid

        if doc is None:
            raise ValueError(f"Could not find Resource with uid={uid}")
        if "resource" in self.transforms:
            transformed_doc = self.transforms["resource"](doc)
        else:
            transformed_doc = doc
        return transformed_doc

    def lookup_resource_for_datum(self, datum_id):
        doc = self._datum_collection.find_one({"datum_id": datum_id})
        if doc is None:
            raise ValueError(f"Could not find Datum with datum_id={datum_id}")
        return doc["resource"]

    def _single_documents(self, fill):
        if fill:
            raise NotImplementedError("Only fill=False is implemented.")
        external_fields = {}  # map descriptor uid to set of external fields
        datum_cache = {}  # map datum_id to datum document
        # Track which Resource and Datum documents we have yielded so far.
        resource_uids = set()
        datum_ids = set()
        # Interleave the documents from the streams in time order.
        merged_iter = toolz.itertoolz.merge_sorted(
            *(stream.iter_descriptors_and_events() for stream in self.values()),
            key=lambda item: item[1]["time"],
        )
        yield ("start", self.metadata["start"])
        for name, doc in merged_iter:
            # Insert Datum, Resource as needed, and then yield (name, doc).
            if name == "event":
                for field in external_fields[doc["descriptor"]]:
                    datum_id = doc["data"][field]
                    if datum_ids not in datum_ids:
                        # We haven't yielded this Datum yet. Look it up, and yield it.
                        try:
                            # Check to see if it's been pre-fetched.
                            datum = datum_cache.pop(datum_id)
                        except KeyError:
                            resource_uid = self.lookup_resource_for_datum(datum_id)
                            if resource_uid not in resource_uids:
                                # We haven't yielded this Resource yet. Look it up, and yield it.
                                resource = self.get_resource(resource_uid)
                                resource_uids.add(resource_uid)
                                yield ("resource", resource)
                                # Pre-fetch *all* the Datum documents for this resource in one query.
                                datum_cache.update(
                                    {
                                        doc["datum_id"]: doc
                                        for doc in self.get_datum_for_resource(
                                            resource_uid
                                        )
                                    }
                                )
                                # Now get the Datum we originally were looking for.
                                datum = datum_cache.pop(datum_id)
                            datum_ids.add(datum_id)
                        yield ("datum", datum)
            elif name == "descriptor":
                # Track which fields ("data keys") hold references to external data.
                external_fields[doc["uid"]] = {
                    key
                    for key, value in doc["data_keys"].items()
                    if value.get("external")
                }
            yield name, doc
        stop_doc = self.metadata["stop"]
        if stop_doc is not None:
            yield ("stop", stop_doc)

    def documents(self, fill, size=25):
        """
        Yield ``(name, document)`` items from the run.

        Batch Event and Datum documents into pages of up to ``size`` rows,
        while preserving time-ordering.
        """
        yield from batch_documents(self._single_documents(fill=fill), size)


class BlueskyEventStream(TreeInMemory, BlueskyEventStreamMixin):
    specs = ["BlueskyEventStream"]

    def __init__(self, *args, event_collection, cutoff_seq_num, run, **kwargs):
        super().__init__(*args, **kwargs)
        self._event_collection = event_collection
        self._cutoff_seq_num = cutoff_seq_num
        self._run = run

    @property
    def metadata(self):
        # If there are transforms configured, shadow the 'descriptor' documents
        # with transfomed copies.
        transformed = {}
        transforms = self._run.transforms
        if "descriptor" in transforms:
            transformed["descriptors"] = [
                transforms["descriptor"](d) for d in self._metadata["descriptors"]
            ]
        metadata = collections.ChainMap(transformed, self._metadata)
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(metadata)

    def new_variation(self, **kwargs):
        return super().new_variation(
            event_collection=self._event_collection,
            cutoff_seq_num=self._cutoff_seq_num,
            run=self._run,
            **kwargs,
        )

    def iter_descriptors_and_events(self):
        for descriptor in sorted(self.metadata["descriptors"], key=lambda d: d["time"]):
            yield ("descriptor", descriptor)
            # TODO Grab paginated chunks.
            events = list(
                self._event_collection.find(
                    {
                        "descriptor": descriptor["uid"],
                        "seq_num": {"$lte": self._cutoff_seq_num},
                    },
                    {"_id": False},
                    sort=[("time", pymongo.ASCENDING)],
                )
            )
            for event in events:
                yield ("event", event)


class DatasetFromDocuments:
    """
    An xarray.Dataset from a sub-dict of an Event stream
    """

    structure_family = "dataset"

    def __init__(
        self,
        *,
        run,
        stream_name,
        cutoff_seq_num,
        event_descriptors,
        event_collection,
        root_map,
        sub_dict,
        metadata=None,
    ):
        self._run = run
        self._stream_name = stream_name
        self._cutoff_seq_num = cutoff_seq_num
        self._event_descriptors = event_descriptors
        self._event_collection = event_collection
        self._sub_dict = sub_dict
        self.root_map = root_map

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def metadata(self):
        return self._run[self._stream_name].metadata

    def macrostructure(self):
        # The `data_keys` in a series of Event Descriptor documents with the same
        # `name` MUST be alike, so we can choose one arbitrarily.
        # IMPORTANT: Access via self.metadata so that the transforms are applied.
        descriptor, *_ = self.metadata["descriptors"]
        data_vars = {}
        dim_counter = itertools.count()
        # Collect the keys (column names) that are of unicode data type.
        unicode_keys = []
        for key, field_metadata in descriptor["data_keys"].items():
            if self._sub_dict == "data":
                unicode_keys.append(key)
        # Load the all the data for unicode columns to figure out the itemsize.
        # We have no other choice, except to *guess* but we'd be in
        # trouble if our guess were too small, and we'll waste space
        # if our guess is too large.
        columns = self._get_columns(unicode_keys, slices=None)  # Fetch *all*.
        for key, field_metadata in descriptor["data_keys"].items():
            # if the EventDescriptor doesn't provide names for the
            # dimensions (it's optional) use the same default dimension
            # names that xarray would.
            try:
                dims = ["time"] + field_metadata["dims"]
            except KeyError:
                ndim = len(field_metadata["shape"])
                dims = ["time"] + [f"dim_{next(dim_counter)}" for _ in range(ndim)]
            attrs = {}
            # Record which object (i.e. device) this column is associated with,
            # which enables one to find the relevant configuration, if any.
            for object_name, keys_ in descriptor.get("object_keys", {}).items():
                for item in keys_:
                    if item == key:
                        attrs["object"] = object_name
                        break
            units = field_metadata.get("units")
            if units:
                if isinstance(units, str):
                    attrs["units_string"] = units
                # TODO We may soon add a more structured units type, which
                # would likely be a dict here.
            if self._sub_dict == "data":
                shape = tuple((self._cutoff_seq_num - 1, *field_metadata["shape"]))
                dtype = JSON_DTYPE_TO_MACHINE_DATA_TYPE[field_metadata["dtype"]]
                if dtype.kind == Kind.unicode:
                    array = columns[key]
                    # I do not fully understand why we need this factor of 4.
                    # Something about what itemsize means to the dtype system
                    # versus its actual bytesize.
                    # This might be making an assumption that we have ASCII
                    # characters only, which is not a safe assumption. We should
                    # revisit this.
                    dtype.itemsize = array.itemsize // 4
            else:
                # assert sub_dict == "timestamps"
                shape = tuple((self._cutoff_seq_num - 1,))
                dtype = FLOAT_DTYPE
            # TEMP: Special-case 4D data in a way that optimzes single-frame
            # access of area detector data. The correct way to handle this may
            # be to place "chunks" in the data_keys.
            if len(shape) == 4:
                # If we choose 1 that would make single-frame access fast
                # but many-frame access too slow.
                suggested_chunks = (
                    min(10, shape[0]),
                    min(10, shape[1]),
                    "auto",
                    "auto",
                )
            else:
                suggested_chunks = ("auto",) * len(shape)
            chunks = normalize_chunks(
                suggested_chunks,
                shape=shape,
                limit=CHUNK_SIZE_LIMIT,
                dtype=dtype.to_numpy_dtype(),
            )
            data = ArrayStructure(
                macro=ArrayMacroStructure(shape=shape, chunks=chunks),
                micro=dtype,
            )
            variable = VariableStructure(
                macro=VariableMacroStructure(dims=dims, data=data, attrs=attrs),
                micro=None,
            )
            data_array = DataArrayStructure(
                macro=DataArrayMacroStructure(variable, coords={}, name=key), micro=None
            )
            data_vars[key] = data_array
        # Build the time coordinate.
        shape = (self._cutoff_seq_num - 1,)
        chunks = normalize_chunks(
            ("auto",) * len(shape),
            shape=shape,
            limit=CHUNK_SIZE_LIMIT,
            dtype=FLOAT_DTYPE.to_numpy_dtype(),
        )
        data = ArrayStructure(
            macro=ArrayMacroStructure(
                shape=shape,
                chunks=chunks,
            ),
            micro=FLOAT_DTYPE,
        )
        variable = VariableStructure(
            macro=VariableMacroStructure(dims=["time"], data=data, attrs={}), micro=None
        )
        return DatasetMacroStructure(
            data_vars=data_vars, coords={"time": variable}, attrs={}
        )

    def microstructure(self):
        return None

    def read(self, variables=None):
        # num_blocks = (range(len(n)) for n in chunks)
        # for block in itertools.product(*num_blocks):
        structure = self.macrostructure()
        data_arrays = {}
        keys = list(structure.data_vars)
        columns = self._get_columns(keys, slices=None)
        for key, data_array in structure.data_vars.items():
            if (variables is not None) and (key not in variables):
                continue
            variable = structure.data_vars[key].macro.variable
            dtype = variable.macro.data.micro.to_numpy_dtype()
            raw_array = columns[key]
            array = raw_array.astype(dtype)
            data_array = xarray.DataArray(
                array, attrs=variable.macro.attrs, dims=variable.macro.dims
            )
            data_arrays[key] = data_array
        # Build the time coordinate.
        variable = structure.coords["time"]
        time_coord = self._get_time_coord(slice=None)
        return xarray.Dataset(data_arrays, coords={"time": time_coord})

    def read_variable(self, variable):
        return self.read(variables=[variable])[variable]

    def read_block(self, variable, block, coord=None, slice=None):
        structure = self.macrostructure()
        if coord is not None:
            # The DataArrays generated from Events never have coords.
            raise KeyError(coord)
        if variable == "time":
            data_structure = structure.coords["time"].macro.data
            chunks = data_structure.macro.chunks
            cumdims = [cached_cumsum(bds, initial_zero=True) for bds in chunks]
            slices_for_chunks = [
                [builtins.slice(s, s + dim) for s, dim in zip(starts, shapes)]
                for starts, shapes in zip(cumdims, chunks)
            ]
            (slice_,) = [s[index] for s, index in zip(slices_for_chunks, block)]
            return self._get_time_coord(slice=slice_)
        dtype = structure.data_vars[
            variable
        ].macro.variable.macro.data.micro.to_numpy_dtype()
        data_structure = structure.data_vars[variable].macro.variable.macro.data
        dtype = data_structure.micro.to_numpy_dtype()
        chunks = data_structure.macro.chunks
        cumdims = [cached_cumsum(bds, initial_zero=True) for bds in chunks]
        slices_for_chunks = [
            [builtins.slice(s, s + dim) for s, dim in zip(starts, shapes)]
            for starts, shapes in zip(cumdims, chunks)
        ]
        slices = [s[index] for s, index in zip(slices_for_chunks, block)]
        raw_array = self._get_columns([variable], slices=slices)[variable]
        array = raw_array.astype(dtype)
        if slice is not None:
            array = array[slice]
        return array

    def _get_time_coord(self, slice):
        if slice is None:
            min_seq_num = 1
            max_seq_num = self._cutoff_seq_num
        else:
            min_seq_num = 1 + slice.start
            max_seq_num = 1 + slice.stop
        column = []
        for descriptor in sorted(self.metadata["descriptors"], key=lambda d: d["time"]):
            (result,) = self._event_collection.aggregate(
                [
                    # Select Events for this Descriptor with the appropriate seq_num range.
                    {
                        "$match": {
                            "descriptor": descriptor["uid"],
                            "seq_num": {"$gte": min_seq_num, "$lt": max_seq_num},
                        },
                    },
                    # Sort by time.
                    {"$sort": {"time": 1}},
                    # If seq_num is repeated, take the latest one.
                    {
                        "$group": {
                            "_id": "$seq_num",
                            "doc": {"$last": "$$ROOT"},
                        },
                    },
                    # Re-sort, now by seq_num which *should* be equivalent to
                    # sorting by time but could not be in weird cases
                    # (which I'm not aware have ever occurred) where an NTP sync
                    # moves system time backward mid-run.
                    {"$sort": {"doc.seq_num": 1}},
                    # Extract the column of interest as an array.
                    {
                        "$group": {
                            "_id": "$descriptor",
                            "column": {"$push": "$doc.time"},
                        },
                    },
                ]
            )
            column.extend(result["column"])
        return numpy.array(column)

    def _get_columns(self, keys, slices):
        columns = {key: [] for key in keys}
        if slices is None:
            min_seq_num = 1
            max_seq_num = self._cutoff_seq_num
        else:
            slice_ = slices[0]
            min_seq_num = 1 + slice_.start
            max_seq_num = 1 + slice_.stop
        # IMPORTANT: Access via self.metadata so that transforms are applied.
        descriptors = self.metadata["descriptors"]
        # The `data_keys` in a series of Event Descriptor documents with the
        # same `name` MUST be alike, so we can just use the first one.
        data_keys = [descriptors[0]["data_keys"][key] for key in keys]
        is_externals = ["external" in data_key for data_key in data_keys]
        expected_shapes = [tuple(data_key["shape"] or []) for data_key in data_keys]
        # If data is external, we now have a column of datum_ids, and we need
        # to look up the data that they reference.
        for descriptor in sorted(descriptors, key=lambda d: d["time"]):
            # TODO When seq_num is repeated, take the last one only (sorted by
            # time).
            (result,) = self._event_collection.aggregate(
                [
                    # Select Events for this Descriptor with the appropriate seq_num range.
                    {
                        "$match": {
                            "descriptor": descriptor["uid"],
                            "seq_num": {"$gte": min_seq_num, "$lt": max_seq_num},
                        },
                    },
                    # Sort by time.
                    {"$sort": {"time": 1}},
                    # If seq_num is repeated, take the latest one.
                    {
                        "$group": {
                            "_id": "$seq_num",
                            "doc": {"$last": "$$ROOT"},
                        },
                    },
                    # Re-sort, now by seq_num which *should* be equivalent to
                    # sorting by time but could not be in weird cases
                    # (which I'm not aware have ever occurred) where an NTP sync
                    # moves system time backward mid-run.
                    {"$sort": {"doc.seq_num": 1}},
                    # Extract the column of interest as an array.
                    {
                        "$group": {
                            "_id": "$descriptor",
                            **{
                                key: {"$push": f"$doc.{self._sub_dict}.{key}"}
                                for key in keys
                            },
                        },
                    },
                ]
            )
            for key, expected_shape, is_external in zip(
                keys, expected_shapes, is_externals
            ):
                if expected_shape and (not is_external):
                    validated_column = list(
                        map(
                            lambda item: _validate_shape(
                                numpy.asarray(item), expected_shape
                            ),
                            result[key],
                        )
                    )
                else:
                    validated_column = result[key]
                columns[key].extend(validated_column)

        result = {}
        for key, expected_shape, is_external in zip(
            keys, expected_shapes, is_externals
        ):
            column = columns[key]
            if is_external:
                filled_column = []
                descriptor_uid = descriptor["uid"]
                for datum_id in column:
                    # HACK to adapt Filler which is designed to consume whole,
                    # streamed documents, to this column-based access mode.
                    mock_event = {
                        "data": {key: datum_id},
                        "descriptor": descriptor_uid,
                        "uid": "PLACEHOLDER",
                        "filled": {key: False},
                    }
                    filled_mock_event = _fill(
                        self._run.filler,
                        mock_event,
                        self._run.lookup_resource_for_datum,
                        self._run.get_resource,
                        self._run.get_datum_for_resource,
                        last_datum_id=None,
                    )
                    filled_data = filled_mock_event["data"][key]
                    validated_filled_data = _validate_shape(filled_data, expected_shape)
                    filled_column.append(validated_filled_data)
                to_stack = filled_column
            else:
                to_stack = column
            array = numpy.stack(to_stack)
            if slices:
                sliced_array = array[(..., *slices[1:])]
            else:
                sliced_array = array
            result[key] = sliced_array
        return result


class Config(TreeInMemory):
    """
    Tree of configuration datasets, keyed on 'object' (e.g. device)
    """

    ...


class ConfigDatasetFromDocuments(DatasetFromDocuments):
    """
    An xarray.Dataset from a configuration sub-dict of an Event stream
    """

    structure_family = "dataset"

    def __init__(
        self,
        *,
        run,
        stream_name,
        cutoff_seq_num,
        event_descriptors,
        event_collection,
        sub_dict,
        object_name,
    ):
        super().__init__(
            run=run,
            stream_name=stream_name,
            cutoff_seq_num=cutoff_seq_num,
            event_descriptors=event_descriptors,
            event_collection=event_collection,
            sub_dict=sub_dict,
        )
        self._object_name = object_name

    def macrostructure(self):
        # The `data_keys` in a series of Event Descriptor documents with the same
        # `name` MUST be alike, so we can choose one arbitrarily.
        # IMPORTANT: Access via self.metadata so that the transforms are applied.
        descriptor, *_ = self.metadata["descriptors"]
        data_vars = {}
        dim_counter = itertools.count()
        data_keys = (
            descriptor.get("configuration", {})
            .get(self._object_name, {})
            .get("data_keys", {})
        )
        # Collect the keys (column names) that are of unicode data type.
        unicode_keys = []
        for key, field_metadata in descriptor["data_keys"].items():
            if self._sub_dict == "data":
                unicode_keys.append(key)
        # Load the all the data for unicode columns to figure out the itemsize.
        # We have no other choice, except to *guess* but we'd be in
        # trouble if our guess were too small, and we'll waste space
        # if our guess is too large.
        columns = self._get_columns(unicode_keys, slices=None)  # Fetch *all*.
        for key, field_metadata in data_keys.items():
            # if the EventDescriptor doesn't provide names for the
            # dimensions (it's optional) use the same default dimension
            # names that xarray would.
            try:
                dims = ["time"] + field_metadata["dims"]
            except KeyError:
                ndim = len(field_metadata["shape"])
                dims = ["time"] + [f"dim_{next(dim_counter)}" for _ in range(ndim)]
            attrs = {}
            units = field_metadata.get("units")
            if units:
                if isinstance(units, str):
                    attrs["units_string"] = units
                # TODO We may soon add a more structured units type, which
                # would likely be a dict here.
            if self._sub_dict == "data":
                shape = tuple((self._cutoff_seq_num - 1, *field_metadata["shape"]))
                dtype = JSON_DTYPE_TO_MACHINE_DATA_TYPE[field_metadata["dtype"]]
                if dtype.kind == Kind.unicode:
                    array = columns[key]
                    # I do not fully understand why we need this factor of 4.
                    # Something about what itemsize means to the dtype system
                    # versus its actual bytesize.
                    dtype.itemsize = array.itemsize // 4
            else:
                # assert sub_dict == "timestamps"
                shape = tuple((self._cutoff_seq_num - 1,))
                dtype = FLOAT_DTYPE
            suggested_chunks = ("auto",) * len(shape)
            chunks = normalize_chunks(
                suggested_chunks,
                shape=shape,
                limit=CHUNK_SIZE_LIMIT,
                dtype=dtype.to_numpy_dtype(),
            )
            data = ArrayStructure(
                macro=ArrayMacroStructure(shape=shape, chunks=chunks),
                micro=dtype,
            )
            variable = VariableStructure(
                macro=VariableMacroStructure(dims=dims, data=data, attrs=attrs),
                micro=None,
            )
            data_array = DataArrayStructure(
                macro=DataArrayMacroStructure(variable, coords={}, name=key), micro=None
            )
            data_vars[key] = data_array
        # Build the time coordinate.
        shape = (self._cutoff_seq_num,)
        chunks = normalize_chunks(
            ("auto",) * len(shape),
            shape=shape,
            limit=CHUNK_SIZE_LIMIT,
            dtype=FLOAT_DTYPE.to_numpy_dtype(),
        )
        data = ArrayStructure(
            macro=ArrayMacroStructure(
                shape=shape,
                chunks=chunks,
            ),
            micro=FLOAT_DTYPE,
        )
        variable = VariableStructure(
            macro=VariableMacroStructure(dims=["time"], data=data, attrs={}), micro=None
        )
        return DatasetMacroStructure(
            data_vars=data_vars, coords={"time": variable}, attrs={}
        )


class Tree(collections.abc.Mapping, CatalogOfBlueskyRunsMixin, IndexersMixin):
    specs = ["CatalogOfBlueskyRuns"]

    # Define classmethods for managing what queries this Tree knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    # Add a /documents route to the server.
    include_routers = [router]

    @classmethod
    def from_uri(
        cls,
        uri,
        *,
        asset_registry_uri=None,
        handler_registry=None,
        root_map=None,
        transforms=None,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
        cache_ttl_complete=60,  # seconds
        cache_ttl_partial=2,  # seconds
    ):
        """
        Create a Tree from MongoDB with the "normalized" (original) layout.

        Parameters
        ----------
        uri: str
            MongoDB URI with database name, formed like "mongodb://HOSTNAME:27017/DATABASE_NAME"
            or "mongodb://USER:PASSWORD@HOSTNAME:27017/DATABASE_NAME?authSource=admin".
        asset_registry_uri: str
            A separate URI for legacy deployments that place Resource and Datum documents
            in a different database from the rest of the documents. (This is believed to
            only be needed for old deployments at BNL NSLS-II.)
        handler_registry: dict, optional
            Maps each 'spec' (a string identifying a given type or external
            resource) to a handler class.

            A 'handler class' may be any callable with the signature::

                handler_class(resource_path, root, **resource_kwargs)

            It is expected to return an object, a 'handler instance', which is also
            callable and has the following signature::

                handler_instance(**datum_kwargs)

            As the names 'handler class' and 'handler instance' suggest, this is
            typically implemented using a class that implements ``__init__`` and
            ``__call__``, with the respective signatures. But in general it may be
            any callable-that-returns-a-callable.
        root_map: dict, optional
            str -> str mapping to account for temporarily moved/copied/remounted
            files.  Any resources which have a ``root`` in ``root_map`` will be
            loaded using the mapped ``root``.
        transforms: dict
            A dict that maps any subset of the keys {start, stop, resource, descriptor}
            to a function that accepts a document of the corresponding type and
            returns it, potentially modified. This feature is for patching up
            erroneous metadata. It is intended for quick, temporary fixes that
            may later be applied permanently to the data at rest
            (e.g., via a database migration).
        cache_ttl_partial : float
            Time (in seconds) to cache a *partial* (i.e. incomplete, ongoing)
            BlueskyRun before checking the database for updates. Default 2.
        cache_ttl_complete : float
            Time (in seconds) to cache a *complete* BlueskyRun before checking
            the database for updates. Default 60.
        """
        metadatastore_db = _get_database(uri)
        if asset_registry_uri is None:
            asset_registry_db = metadatastore_db
        else:
            asset_registry_db = _get_database(asset_registry_uri)
        root_map = root_map or {}
        transforms = parse_transforms(transforms)
        if handler_registry is None:
            handler_registry = discover_handlers()
        handler_registry = parse_handler_registry(handler_registry)
        # Two different caches with different eviction rules.
        cache_of_complete_bluesky_runs = cachetools.TTLCache(
            ttl=cache_ttl_complete, maxsize=100
        )
        cache_of_partial_bluesky_runs = cachetools.TTLCache(
            ttl=cache_ttl_partial, maxsize=100
        )
        return cls(
            metadatastore_db=metadatastore_db,
            asset_registry_db=asset_registry_db,
            handler_registry=handler_registry,
            root_map=root_map,
            transforms=transforms,
            cache_of_complete_bluesky_runs=cache_of_complete_bluesky_runs,
            cache_of_partial_bluesky_runs=cache_of_partial_bluesky_runs,
            metadata=metadata,
            access_policy=access_policy,
            authenticated_identity=authenticated_identity,
        )

    @classmethod
    def from_mongomock(
        cls,
        *,
        handler_registry=None,
        root_map=None,
        transforms=None,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
        cache_ttl_complete=60,  # seconds
        cache_ttl_partial=2,  # seconds
    ):
        """
        Create a transient Tree from backed by "mongomock".

        This is intended for testing, teaching, an demos. The data does not
        persistent. Do not use this for anything important.

        Parameters
        ----------
        handler_registry: dict, optional
            Maps each 'spec' (a string identifying a given type or external
            resource) to a handler class.

            A 'handler class' may be any callable with the signature::

                handler_class(resource_path, root, **resource_kwargs)

            It is expected to return an object, a 'handler instance', which is also
            callable and has the following signature::

                handler_instance(**datum_kwargs)

            As the names 'handler class' and 'handler instance' suggest, this is
            typically implemented using a class that implements ``__init__`` and
            ``__call__``, with the respective signatures. But in general it may be
            any callable-that-returns-a-callable.
        root_map: dict, optional
            str -> str mapping to account for temporarily moved/copied/remounted
            files.  Any resources which have a ``root`` in ``root_map`` will be
            loaded using the mapped ``root``.
        transforms: dict
            A dict that maps any subset of the keys {start, stop, resource, descriptor}
            to a function that accepts a document of the corresponding type and
            returns it, potentially modified. This feature is for patching up
            erroneous metadata. It is intended for quick, temporary fixes that
            may later be applied permanently to the data at rest
            (e.g., via a database migration).
        cache_ttl_partial : float
            Time (in seconds) to cache a *partial* (i.e. incomplete, ongoing)
            BlueskyRun before checking the database for updates. Default 2.
        cache_ttl_complete : float
            Time (in seconds) to cache a *complete* BlueskyRun before checking
            the database for updates. Default 60.
        """
        import mongomock

        client = mongomock.MongoClient()
        metadatastore_db = asset_registry_db = client["mock_database"]
        root_map = root_map or {}
        transforms = parse_transforms(transforms)
        if handler_registry is None:
            handler_registry = discover_handlers()
        handler_registry = parse_handler_registry(handler_registry)
        # Two different caches with different eviction rules.
        cache_of_complete_bluesky_runs = cachetools.TTLCache(
            ttl=cache_ttl_complete, maxsize=100
        )
        cache_of_partial_bluesky_runs = cachetools.TTLCache(
            ttl=cache_ttl_partial, maxsize=100
        )
        return cls(
            metadatastore_db=metadatastore_db,
            asset_registry_db=asset_registry_db,
            handler_registry=handler_registry,
            root_map=root_map,
            transforms=transforms,
            cache_of_complete_bluesky_runs=cache_of_complete_bluesky_runs,
            cache_of_partial_bluesky_runs=cache_of_partial_bluesky_runs,
            metadata=metadata,
            access_policy=access_policy,
            authenticated_identity=authenticated_identity,
        )

    def __init__(
        self,
        metadatastore_db,
        asset_registry_db,
        handler_registry,
        root_map,
        transforms,
        cache_of_complete_bluesky_runs,
        cache_of_partial_bluesky_runs,
        metadata=None,
        queries=None,
        sorting=None,
        access_policy=None,
        authenticated_identity=None,
    ):
        "This is not user-facing. Use Tree.from_uri."
        self._run_start_collection = metadatastore_db.get_collection("run_start")
        self._run_stop_collection = metadatastore_db.get_collection("run_stop")
        self._event_descriptor_collection = metadatastore_db.get_collection(
            "event_descriptor"
        )
        self._event_collection = metadatastore_db.get_collection("event")
        self._resource_collection = asset_registry_db.get_collection("resource")
        self._datum_collection = asset_registry_db.get_collection("datum")
        self._metadatastore_db = metadatastore_db
        self._asset_registry_db = asset_registry_db

        self._handler_registry = handler_registry
        self.handler_registry = event_model.HandlerRegistryView(self._handler_registry)
        self.root_map = root_map
        self.transforms = transforms or {}
        self._cache_of_complete_bluesky_runs = cache_of_complete_bluesky_runs
        self._cache_of_partial_bluesky_runs = cache_of_partial_bluesky_runs
        self._cache_of_bluesky_runs = collections.ChainMap(
            cache_of_complete_bluesky_runs, cache_of_partial_bluesky_runs
        )
        self._metadata = metadata or {}
        self.queries = list(queries or [])
        if sorting is None:
            sorting = [("time", 1)]
        self._sorting = sorting
        if (access_policy is not None) and (
            not access_policy.check_compatibility(self)
        ):
            raise ValueError(
                f"Access policy {access_policy} is not compatible with this Tree."
            )
        self._access_policy = access_policy
        self._authenticated_identity = authenticated_identity
        super().__init__()

    @property
    def database(self):
        """
        The underlying MongoDB database object

        Note: Very old deployments use two separate databases. We believe these
        are rare "in the wild", mostly or entirely restricted to the earliest
        facility to use databroker, BNL NSLS-II.

        In the event that two databases are used, this raises
        NotImplementedError and instructs the callers to use two semi-internal
        attributes instead.
        """
        if self._metadatastore_db != self._asset_registry_db:
            raise NotImplementedError(
                "This Tree is backed by two databases. This is no longer "
                "necessary or recommended. As a result, the `database` property "
                "is undefined. Use the attributes _metadatastore_db and "
                "_asset_registry_db directly."
            )
        return self._metadatastore_db

    def register_handler(self, spec, handler, overwrite=False):
        if (not overwrite) and (spec in self._handler_registry):
            original = self._handler_registry[spec]
            if original is handler:
                return
            raise event_model.DuplicateHandler(
                f"There is already a handler registered for the spec {spec!r}. "
                f"Use overwrite=True to deregister the original.\n"
                f"Original: {original}\n"
                f"New: {handler}"
            )
        self._handler_registry[spec] = handler

    def deregister_handler(self, spec):
        self._handler_registry.pop(spec, None)

    def new_variation(
        self,
        *args,
        metadata=UNCHANGED,
        queries=UNCHANGED,
        sorting=UNCHANGED,
        authenticated_identity=UNCHANGED,
        **kwargs,
    ):
        if metadata is UNCHANGED:
            metadata = self._metadata
        if queries is UNCHANGED:
            queries = self.queries
        if sorting is UNCHANGED:
            sorting = self._sorting
        if authenticated_identity is UNCHANGED:
            authenticated_identity = self._authenticated_identity
        return type(self)(
            *args,
            metadatastore_db=self._metadatastore_db,
            asset_registry_db=self._asset_registry_db,
            handler_registry=self.handler_registry,
            root_map=self.root_map,
            transforms=self.transforms,
            cache_of_complete_bluesky_runs=self._cache_of_complete_bluesky_runs,
            cache_of_partial_bluesky_runs=self._cache_of_partial_bluesky_runs,
            queries=queries,
            sorting=sorting,
            access_policy=self.access_policy,
            authenticated_identity=authenticated_identity,
            **kwargs,
        )

    @property
    def access_policy(self):
        return self._access_policy

    @property
    def authenticated_identity(self):
        return self._authenticated_identity

    @property
    def metadata(self):
        "Metadata about this Tree."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    @property
    def sorting(self):
        return list(self._sorting)

    def __repr__(self):
        # Display up to the first N keys to avoid making a giant service
        # request. Use _keys_slicer because it is unauthenticated.
        N = 10
        return tree_repr(self, self._keys_slice(0, N, direction=1))

    def _get_run(self, run_start_doc):
        "Get a BlueskyRun, either from a cache or by making one if needed."
        uid = run_start_doc["uid"]
        try:
            return self._cache_of_bluesky_runs[uid]
        except KeyError:
            run = self._build_run(run_start_doc)
            # Choose a cache depending on whethter the run is complete (in
            # which case updates are rare) or incomplete/partial (in which case
            # more data is likely incoming soon).
            if run.metadata.get("stop") is None:
                self._cache_of_partial_bluesky_runs[uid] = run
            else:
                self._cache_of_complete_bluesky_runs[uid] = run
            return run

    def _build_run(self, run_start_doc):
        "This should not be called directly, even internally. Use _get_run."
        # Instantiate a BlueskyRun for this run_start_doc.
        uid = run_start_doc["uid"]
        # This may be None; that's fine.
        run_stop_doc = self._get_stop_doc(uid)
        stream_names = self._event_descriptor_collection.distinct(
            "name",
            {"run_start": uid},
        )
        mapping = {}
        for stream_name in stream_names:
            mapping[stream_name] = functools.partial(
                self._build_event_stream,
                run_start_uid=uid,
                stream_name=stream_name,
                is_complete=(run_stop_doc is not None),
            )
        return BlueskyRun(
            OneShotCachedMap(mapping),
            metadata={"start": run_start_doc, "stop": run_stop_doc},
            handler_registry=self.handler_registry,
            transforms=copy.copy(self.transforms),
            root_map=copy.copy(self.root_map),
            datum_collection=self._datum_collection,
            resource_collection=self._resource_collection,
        )

    def _build_event_stream(self, *, run_start_uid, stream_name, is_complete):
        event_descriptors = list(
            self._event_descriptor_collection.find(
                {"run_start": run_start_uid, "name": stream_name}, {"_id": False}
            )
        )
        event_descriptor_uids = [doc["uid"] for doc in event_descriptors]
        # We need each of the sub-dicts to have a consistent length. If
        # Events are still being added, we need to choose a consistent
        # cutoff. If not, we need to know the length anyway. Note that this
        # is not the same thing as the number of Event documents in the
        # stream because seq_num may be repeated, nonunique.
        (result,) = self._event_collection.aggregate(
            [
                {"$match": {"descriptor": {"$in": event_descriptor_uids}}},
                {
                    "$group": {
                        "_id": "descriptor",
                        "highest_seq_num": {"$max": "$seq_num"},
                    },
                },
            ]
        )
        cutoff_seq_num = (
            1 + result["highest_seq_num"]
        )  # `1 +` because we use a half-open interval
        object_names = event_descriptors[0]["object_keys"]
        run = self[run_start_uid]
        mapping = OneShotCachedMap(
            {
                "data": lambda: DatasetFromDocuments(
                    run=run,
                    stream_name=stream_name,
                    cutoff_seq_num=cutoff_seq_num,
                    event_descriptors=event_descriptors,
                    event_collection=self._event_collection,
                    root_map=self.root_map,
                    sub_dict="data",
                ),
                "timestamps": lambda: DatasetFromDocuments(
                    run=run,
                    stream_name=stream_name,
                    cutoff_seq_num=cutoff_seq_num,
                    event_descriptors=event_descriptors,
                    event_collection=self._event_collection,
                    root_map=self.root_map,
                    sub_dict="timestamps",
                ),
                "config": lambda: Config(
                    OneShotCachedMap(
                        {
                            object_name: lambda object_name=object_name: ConfigDatasetFromDocuments(
                                run=run,
                                stream_name=stream_name,
                                cutoff_seq_num=cutoff_seq_num,
                                event_descriptors=event_descriptors,
                                event_collection=self._event_collection,
                                object_name=object_name,
                                sub_dict="data",
                            )
                            for object_name in object_names
                        }
                    )
                ),
                "config_timestamps": lambda: Config(
                    OneShotCachedMap(
                        {
                            object_name: lambda object_name=object_name: ConfigDatasetFromDocuments(
                                run=run,
                                stream_name=stream_name,
                                cutoff_seq_num=cutoff_seq_num,
                                event_descriptors=event_descriptors,
                                event_collection=self._event_collection,
                                object_name=object_name,
                                sub_dict="timestamps",
                            )
                            for object_name in object_names
                        }
                    )
                ),
            }
        )

        metadata = {"descriptors": event_descriptors, "stream_name": stream_name}
        return BlueskyEventStream(
            mapping,
            metadata=metadata,
            event_collection=self._event_collection,
            cutoff_seq_num=cutoff_seq_num,
            run=run,
        )

    def __getitem__(self, key):
        # Lookup this key *within the search results* of this Tree.
        query = self._build_mongo_query({"uid": key})
        run_start_doc = self._run_start_collection.find_one(query, {"_id": False})
        if run_start_doc is None:
            raise KeyError(key)
        return self._get_run(run_start_doc)

    def _chunked_find(self, collection, query, *args, skip=0, limit=None, **kwargs):
        # This is an internal chunking that affects how much we pull from
        # MongoDB at a time.
        CURSOR_LIMIT = 100  # TODO Tune this for performance.
        if limit is not None and limit < CURSOR_LIMIT:
            initial_limit = limit
        else:
            initial_limit = CURSOR_LIMIT
        cursor = (
            collection.find(query, *args, **kwargs)
            .sort(self._sorting + [("_id", 1)])
            .skip(skip)
            .limit(initial_limit)
        )
        # Fetch in batches, starting each batch from where we left off.
        # https://medium.com/swlh/mongodb-pagination-fast-consistent-ece2a97070f3
        tally = 0
        items = []
        last_sorted_values = {}
        while True:
            # Greedily exhaust the cursor. The user may loop over this iterator
            # slowly and, if we don't pull it all into memory now, we'll be
            # holding a long-lived cursor that might get timed out by the
            # MongoDB server.
            items.extend(cursor)  # Exhaust cursor.
            if not items:
                break
            # Next time through the loop, we'll pick up where we left off.
            last_object_id = items[-1]["_id"]
            last_item = items[-1]
            for name, _ in self._sorting:
                # This supports sorting by sub-items like, for example,
                # "XDI.Element.edge".
                first_token, *tokens = name.split(".")
                value = last_item[first_token]
                for token in tokens:
                    value = value[token]
                last_sorted_values[name] = value
                page_cutoff_query = [
                    {
                        "$or": [
                            {
                                name: {
                                    (
                                        "$gt" if direction > 0 else "$lt"
                                    ): last_sorted_values[name]
                                }
                            },
                            {
                                name: last_sorted_values[name],
                                "_id": {"$gt": last_object_id},
                            },
                        ]
                    }
                    for name, direction in self._sorting
                ]
                if "$and" in query:
                    query["$and"].extend(page_cutoff_query)
                else:
                    query["$and"] = page_cutoff_query
            for item in items:
                item.pop("_id")
                yield item
            tally += len(items)
            if limit is not None:
                if tally == limit:
                    break
                if limit - tally < CURSOR_LIMIT:
                    this_limit = limit - tally
                else:
                    this_limit = CURSOR_LIMIT
            else:
                this_limit = CURSOR_LIMIT
            # Get another batch and go round again.
            cursor = (
                collection.find(query, *args, **kwargs)
                .sort(self._sorting + [("_id", 1)])
                .limit(this_limit)
            )
            items.clear()

    def _build_mongo_query(self, *queries):
        combined = self.queries + list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def _get_stop_doc(self, run_start_uid):
        "This may return None."
        return self._run_stop_collection.find_one(
            {"run_start": run_start_uid}, {"_id": False}
        )

    def __iter__(self):
        for run_start_doc in self._chunked_find(
            self._run_start_collection, self._build_mongo_query()
        ):
            yield run_start_doc["uid"]

    def __len__(self):
        return self._run_start_collection.count_documents(self._build_mongo_query())

    def __length_hint__(self):
        # https://www.python.org/dev/peps/pep-0424/
        return self._run_start_collection.estimated_document_count(
            self._build_mongo_query(),
        )

    def authenticated_as(self, identity):
        if self._authenticated_identity is not None:
            raise RuntimeError(
                f"Already authenticated as {self.authenticated_identity}"
            )
        if self._access_policy is not None:
            raise NotImplementedError
        else:
            catalog = self.new_variation()
        return catalog

    def search(self, query):
        """
        Return a Tree with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def sort(self, sorting):
        return self.new_variation(sorting=sorting)

    def _keys_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None
        for run_start_doc in self._chunked_find(
            self._run_start_collection,
            self._build_mongo_query(),
            skip=skip,
            limit=limit,
        ):
            # TODO Fetch just the uid.
            yield run_start_doc["uid"]

    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None
        for run_start_doc in self._chunked_find(
            self._run_start_collection,
            self._build_mongo_query(),
            skip=skip,
            limit=limit,
        ):
            uid = run_start_doc["uid"]
            run = self._get_run(run_start_doc)
            yield (uid, run)

    def _item_by_index(self, index, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        run_start_doc = next(
            self._chunked_find(
                self._run_start_collection,
                self._build_mongo_query(),
                skip=index,
                limit=1,
            )
        )
        uid = run_start_doc["uid"]
        run = self._get_run(run_start_doc)
        return (uid, run)


def full_text_search(query, catalog):
    # First if this catalog is backed by mongomock, which does not support $text queries.
    # Avoid importing mongomock if it is not already imported.
    if "mongomock" in sys.modules:
        import mongomock

        if isinstance(catalog.database.client, mongomock.MongoClient):
            # Do the query in memory.
            # For huge Trees this will be slow, but if you are attempting
            # full text search on a large mongomock-backed Tree,
            # you have made your choices! :-)
            return TreeInMemory(dict(catalog)).search(query)

    return Tree.query_registry(
        RawMongo(
            start={
                "$text": {"$search": query.text, "$caseSensitive": query.case_sensitive}
            }
        ),
        catalog,
    )


def raw_mongo(query, catalog):
    # For now, only handle search on the 'run_start' collection.
    return catalog.new_variation(
        queries=catalog.queries + [json.loads(query.start)],
    )


# These are implementation-specific definitions.
Tree.register_query(FullText, full_text_search)
Tree.register_query(RawMongo, raw_mongo)
# These are generic definitions that use RawMongo internally.
Tree.register_query(KeyLookup, key_lookup)
Tree.register_query(_PartialUID, partial_uid)
Tree.register_query(_ScanID, scan_id)
Tree.register_query(TimeRange, time_range)


class DummyAccessPolicy:
    "Impose no access restrictions."

    def check_compatibility(self, catalog):
        # This only works on in-memory Tree or subclases.
        return isinstance(catalog, Tree)

    def modify_queries(self, queries, authenticated_identity):
        return queries

    def filter_results(self, catalog, authenticated_identity):
        return type(catalog)(
            metadatastore_db=catalog.metadatastore_db,
            asset_registry_db=catalog.asset_registry_db,
            metadata=catalog.metadata,
            access_policy=catalog.access_policy,
            authenticated_identity=authenticated_identity,
        )


class SimpleAccessPolicy:
    """
    Refer to a mapping of user names to lists of entries they can access.

    >>> SimpleAccessPolicy({"alice": ["A", "B"], "bob": ["B"]})
    """

    ALL = object()  # sentinel

    def __init__(self, access_lists):
        self.access_lists = access_lists

    def check_compatibility(self, catalog):
        # This only works on in-memory Tree or subclases.
        return isinstance(catalog, Tree)

    def modify_queries(self, queries, authenticated_identity):
        allowed = self.access_lists.get(authenticated_identity, [])
        if (authenticated_identity is SpecialUsers.admin) or (allowed is self.ALL):
            modified_queries = queries
        else:
            modified_queries = list(queries)
            modified_queries.append(RawMongo(start={"uid": {"$in": allowed}}))
        return modified_queries

    def filter_results(self, catalog, authenticated_identity):
        return catalog.new_variation(authenticated_identity=authenticated_identity)


def _get_database(uri):
    if not pymongo.uri_parser.parse_uri(uri)["database"]:
        raise ValueError(
            f"Invalid URI: {uri!r} " f"Did you forget to include a database?"
        )
    else:
        client = pymongo.MongoClient(uri)
        return client.get_database()


def discover_handlers(entrypoint_group_name="databroker.handlers", skip_failures=True):
    """
    Discover handlers via entrypoints.

    Parameters
    ----------
    entrypoint_group_name: str
        Default is 'databroker.handlers', the "official" databroker entrypoint
        for handlers.
    skip_failures: boolean
        True by default. Errors loading a handler class are converted to
        warnings if this is True.

    Returns
    -------
    handler_registry: dict
        A suitable default handler registry
    """
    group = entrypoints.get_group_named(entrypoint_group_name)
    group_all = entrypoints.get_group_all(entrypoint_group_name)
    if len(group_all) != len(group):
        # There are some name collisions. Let's go digging for them.
        for name, matches in itertools.groupby(group_all, lambda ep: ep.name):
            matches = list(matches)
            if len(matches) != 1:
                winner = group[name]
                warnings.warn(
                    f"There are {len(matches)} entrypoints for the "
                    f"databroker handler spec {name!r}. "
                    f"They are {matches}. The match {winner} has won the race."
                )
    handler_registry = {}
    for name, entrypoint in group.items():
        try:
            handler_class = entrypoint.load()
        except Exception as exc:
            if skip_failures:
                warnings.warn(
                    f"Skipping {entrypoint!r} which failed to load. "
                    f"Exception: {exc!r}"
                )
                continue
            else:
                raise
        handler_registry[name] = handler_class

    return handler_registry


def parse_handler_registry(handler_registry):
    """
    Parse mapping of spec name to 'import path' into mapping to class itself.

    Parameters
    ----------
    handler_registry : dict
        Values may be string 'import paths' to classes or actual classes.

    Examples
    --------
    Pass in name; get back actual class.

    >>> parse_handler_registry({'my_spec': 'package.module.ClassName'})
    {'my_spec': <package.module.ClassName>}

    """
    return _parse_dict_of_objs_or_importable_strings(handler_registry)


def _parse_dict_of_objs_or_importable_strings(d):
    result = {}
    for key, value in d.items():
        if isinstance(value, str):
            try:
                class_ = import_object(value)
            except Exception:
                # For back-compat, trying transforming 'a.b.c' into 'a.b:c'.
                edited_value = ":".join(value.rsplit(".", 1))
                class_ = import_object(edited_value)
                # TODO Warn.
        else:
            class_ = value
        result[key] = class_
    return result


def parse_transforms(transforms):
    """
    Parse mapping of spec name to 'import path' into mapping to class itself.

    Parameters
    ----------
    transforms : collections.abc.Mapping or None
        A collections.abc.Mapping or subclass, that maps any subset of the
        keys {start, stop, resource, descriptor} to a function (or a string
        import path) that accepts a document of the corresponding type and
        returns it, potentially modified. This feature is for patching up
        erroneous metadata. It is intended for quick, temporary fixes that
        may later be applied permanently to the data at rest (e.g via a
        database migration).

    Examples
    --------
    Pass in name; get back actual class.

    >>> parse_transforms({'descriptor': 'package.module.function_name'})
    {'descriptor': <package.module.function_name>}

    """
    if transforms is None:
        return
    elif isinstance(transforms, collections.abc.Mapping):
        allowed_keys = {"start", "stop", "resource", "descriptor"}
        if transforms.keys() - allowed_keys:
            raise NotImplementedError(
                f"Transforms for {transforms.keys() - allowed_keys} "
                f"are not supported."
            )
    else:
        raise ValueError(
            f"Invalid transforms argument {transforms}. "
            f"transforms must be None or a dictionary."
        )
    return _parse_dict_of_objs_or_importable_strings(transforms)


BOOLEAN_DTYPE = MachineDataType.from_numpy_dtype(numpy.dtype("bool"))
FLOAT_DTYPE = MachineDataType.from_numpy_dtype(numpy.dtype("float64"))
INT_DTYPE = MachineDataType.from_numpy_dtype(numpy.dtype("int64"))
STRING_DTYPE = MachineDataType.from_numpy_dtype(numpy.dtype("<U"))
JSON_DTYPE_TO_MACHINE_DATA_TYPE = {
    "boolean": BOOLEAN_DTYPE,
    "number": FLOAT_DTYPE,
    "integer": INT_DTYPE,
    "string": STRING_DTYPE,
    "array": FLOAT_DTYPE,  # HACK This is not a good assumption.
}


def _fill(
    filler,
    event,
    lookup_resource_for_datum,
    get_resource,
    get_datum_for_resource,
    last_datum_id=None,
):
    try:
        _, filled_event = filler("event", event)
        return filled_event
    except event_model.UnresolvableForeignKeyError as err:
        datum_id = err.key
        if datum_id == last_datum_id:
            # We tried to fetch this Datum on the last trip
            # trip through this method, and apparently it did not
            # work. We are in an infinite loop. Bail!
            raise

        # try to fast-path looking up the resource uid if this works
        # it saves us a a database hit (to get the datum document)
        if "/" in datum_id:
            resource_uid, _ = datum_id.split("/", 1)
        # otherwise do it the standard way
        else:
            resource_uid = lookup_resource_for_datum(datum_id)

        # but, it might be the case that the key just happens to have
        # a '/' in it and it does not have any semantic meaning so we
        # optimistically try
        try:
            resource = get_resource(uid=resource_uid)
        # and then fall back to the standard way to be safe
        except ValueError:
            resource = get_resource(lookup_resource_for_datum(datum_id))

        filler("resource", resource)
        # Pre-fetch all datum for this resource.
        for datum in get_datum_for_resource(resource_uid=resource_uid):
            filler("datum", datum)
        # TODO -- When to clear the datum cache in filler?

        # Re-enter and try again now that the Filler has consumed the
        # missing Datum. There might be another missing Datum in this same
        # Event document (hence this re-entrant structure) or might be good
        # to go.
        return _fill(
            filler,
            event,
            lookup_resource_for_datum,
            get_resource,
            get_datum_for_resource,
            last_datum_id=datum_id,
        )


def batch_documents(singles, size):
    # Acculuate rows for Event Pages or Datum Pages in a cache.
    # Drain the cache and emit the page when any of the following conditions
    # are met:
    # (1) We reach a document of a different type.
    # (2) The associated Event Descriptor or Resource changes.
    # (3) The number of rows in the cache reaches `size`.
    cache = collections.deque()
    current_uid = None
    current_type = None

    def handle_item(name, doc):
        nonlocal current_uid
        nonlocal current_type
        if name == "event":
            if (
                (current_type == "event_page")
                and (current_uid == doc["descriptor"])
                and len(cache) < size
            ):
                cache.append(doc)
            elif current_type is None:
                current_type = "event_page"
                current_uid = doc["descriptor"]
                cache.append(doc)
            else:
                # Emit the cache and recurse.
                if current_type == "event_page":
                    yield (current_type, event_model.pack_event_page(*cache))
                if current_type == "datum_page":
                    yield (current_type, event_model.pack_datum_page(*cache))
                cache.clear()
                current_uid = None
                current_type = None
                yield from handle_item(name, doc)
        elif name == "datum":
            if (
                (current_type == "datum_page")
                and (current_uid == doc["resource"])
                and len(cache) < size
            ):
                cache.append(doc)
            elif current_type is None:
                current_type = "datum_page"
                current_uid = doc["resource"]
                cache.append(doc)
            else:
                # Emit the cache and recurse
                if current_type == "event_page":
                    yield (current_type, event_model.pack_event_page(*cache))
                if current_type == "datum_page":
                    yield (current_type, event_model.pack_datum_page(*cache))
                cache.clear()
                current_uid = None
                current_type = None
                yield from handle_item(name, doc)
        else:
            # Emit the cached page (if nonempty) and then this item.
            if cache:
                if current_type == "event_page":
                    yield (current_type, event_model.pack_event_page(*cache))
                elif current_type == "datum_page":
                    yield (current_type, event_model.pack_datum_page(*cache))
                else:
                    assert False
                cache.clear()
                current_uid = None
                current_type = None
            yield (name, doc)

    for name, doc in singles:
        yield from handle_item(name, doc)


class BadShapeMetadata(Exception):
    pass


def _validate_shape(data, expected_shape):
    """
    Check that data.shape == expected.shape.

    * If number of dimensions differ, raise BadShapeMetadata
    * If any dimension is larger than expected, raise BadShapeMetadata.
    * If some dimensions are smaller than expected,, pad "right" edge of each
      dimension that falls short with NaN.
    """
    if data.shape == expected_shape:
        return data
    # Pad at the "end" along any dimension that is too short.
    padding = []
    trimming = []
    for actual, expected in zip(data.shape, expected_shape):
        margin = expected - actual
        # Limit how much padding or trimming we are willing to do.
        SOMEWHAT_ARBITRARY_LIMIT_OF_WHAT_IS_REASONABLE = 2
        if abs(margin) > SOMEWHAT_ARBITRARY_LIMIT_OF_WHAT_IS_REASONABLE:
            raise BadShapeMetadata(
                f"Actual shape {data.shape} does not "
                f"match expected shape {expected_shape}."
            )
        if margin > 0:
            padding.append((0, margin))
            trimming.append(slice(None, None))
        elif margin < 0:
            padding.append((0, 0))
            trimming.append(slice(None))
        else:  # margin == 0
            padding.append((0, 0))
            trimming.append(slice(None, None))
        # TODO Rethink this!
        # We cannot do NaN because that does not work for integers
        # and it is too late to change our mind about the data type.
        padded = numpy.pad(data, padding, "edge")
        padded_and_trimmed = padded[tuple(trimming)]
    return padded_and_trimmed
