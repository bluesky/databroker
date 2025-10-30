import collections
import dataclasses
import os
import re
import warnings
from typing import Literal, Union, cast

import numpy as np
from event_model.documents import EventDescriptor, StreamDatum, StreamResource
from tiled.mimetypes import DEFAULT_ADAPTERS_BY_MIMETYPE
from tiled.structures.array import ArrayStructure, BuiltinDtype, StructDtype
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management


@dataclasses.dataclass
class Patch:
    shape: tuple[int, ...]
    offset: tuple[int, ...]

    @classmethod
    def combine_patches(cls, patches: list["Patch"]) -> "Patch":
        """Combine multiple patches into a single patch

        The combined patch covers the union (smallest bounding box) of all provided patches.

        Parameters
        ----------
        patches : list[Patch]
            A list of Patch objects to combine.

        Returns
        -------
        Patch
            A new Patch object that covers the union of all input patches.
        """

        # Determine the overall shape and offset
        min_offset = list(patches[0].offset)
        max_extent = [offset + size for offset, size in zip(patches[0].offset, patches[0].shape)]

        for patch in patches[1:]:
            for i in range(len(min_offset)):
                min_offset[i] = min(min_offset[i], patch.offset[i])
                max_extent[i] = max(max_extent[i], patch.offset[i] + patch.shape[i])

        combined_shape = tuple(max_e - min_o for min_o, max_e in zip(min_offset, max_extent))
        combined_offset = tuple(min_offset)

        return cls(shape=combined_shape, offset=combined_offset)


class ConsolidatorBase:
    """Consolidator of StreamDatums

    A Consolidator consumes documents from RE; it is similar to usual Bluesky Handlers but is designed to work
    with streaming data (received via StreamResource and StreamDatum documents). It composes details (DataSource
    and its Assets) that will go into the Tiled database. Each Consolidator is instantiated per a Stream Resource.

    Tiled Adapters will later use this to read the data, with good random access and bulk access support.

    We put this code into consolidators so that additional, possibly very unusual, formats can be supported by
    users without getting a PR merged into Bluesky or Tiled.

    The CONSOLIDATOR_REGISTRY (see example below) and the Tiled catalog parameter adapters_by_mimetype can be used
    together to support:
        - Ingesting a new mimetype from Bluesky documents and generating DataSource and Asset with appropriate
          parameters (the consolidator's job);
        - Interpreting those DataSource and Asset parameters to do I/O (the adapter's job).

    To implement new Consolidators for other mimetypes, subclass ConsolidatorBase, possibly expand the
    `consume_stream_datum` and `get_data_source` methods, and ensure that the keys of returned `adapter_parameters`
    dictionary matches the expected adapter signature. Declare a set of supported mimetypes to allow validation and
    automated discovery of the subclassed Consolidator.

    Attributes:
    -----------

    supported_mimetypes : set[str]
        a set of mimetypes that can be handled by a derived Consolidator class; raises ValueError if attempted to
        pass Resource documents related to unsupported mimetypes.
    join_method : Literal["stack", "concat"]
        a method to join the data; if "stack", the resulting consolidated dataset is produced by joining all datums
        along a new dimension added on the left, e.g. a stack of tiff images, otherwise -- datums will be appended
        to the end of the existing leftmost dimension, e.g. rows of a table (similarly to concatenation in numpy).
    join_chunks : bool
        if True, the chunking of the resulting dataset will be determined after consolidation, otherwise each part
        is considered to be chunked separately.
    """

    supported_mimetypes: set[str] = {"application/octet-stream"}
    join_method: Literal["stack", "concat"] = "concat"
    join_chunks: bool = True

    def __init__(self, stream_resource: StreamResource, descriptor: EventDescriptor):
        self.mimetype = self.get_supported_mimetype(stream_resource)

        self.data_key = stream_resource["data_key"]
        self.uri = stream_resource["uri"]
        self.assets: list[Asset] = [Asset(data_uri=self.uri, is_directory=False, parameter="data_uris", num=0)]
        self._sres_parameters = stream_resource["parameters"]

        # Find datum shape and machine dtype
        data_desc = descriptor["data_keys"][self.data_key]
        if None in data_desc["shape"]:
            raise NotImplementedError(f"Consolidator for {self.mimetype} does not support variable-sized data")
        self.datum_shape: tuple[int, ...] = cast(tuple[int, ...], tuple(data_desc["shape"]))
        self.datum_shape = () if self.datum_shape == (1,) and self.join_method == "stack" else self.datum_shape

        # Check that the datum shape is consistent between the StreamResource and the Descriptor
        if multiplier := self._sres_parameters.get("multiplier"):
            self.datum_shape = self.datum_shape or (multiplier,)  # If datum_shape is not set
            if self.datum_shape[0] != multiplier:
                if self.datum_shape[0] == 1:
                    self.datum_shape = (multiplier,) + self.datum_shape[1:]
                else:
                    self.datum_shape = (multiplier,) + self.datum_shape
                    # TODO: Check consistency with chunk_shape

        # Determine the machine data type; fall back to np.dtype("float64") if not set
        self.data_type: Union[BuiltinDtype, StructDtype]
        dtype_descr = data_desc.get("dtype_numpy")
        if isinstance(dtype_descr, list):
            # np.dtype requires tuples in struct dtypes, not lists
            self.data_type = StructDtype.from_numpy_dtype(np.dtype(list(map(tuple, dtype_descr))))
        else:
            self.data_type = BuiltinDtype.from_numpy_dtype(np.dtype(dtype_descr))

        # Set chunk (or partition) shape
        self.chunk_shape = self._sres_parameters.get("chunk_shape", ())
        if any(d <= 0 for d in self.chunk_shape):
            raise ValueError(f"Chunk size in all dimensions must be at least 1: chunk_shape={self.chunk_shape}.")

        # Possibly overwrite the join_method and join_chunks attributes
        self.join_method = self._sres_parameters.get("join_method", self.join_method)
        self.join_chunks = self._sres_parameters.get("join_chunks", self.join_chunks)

        self._num_rows: int = 0  # Number of rows in the Data Source (all rows, includung skips)
        self._seqnums_to_indices_map: dict[int, int] = {}

        # Set the dimension names if provided
        self.dims: tuple[str, ...] = tuple(data_desc.get("dims", ()))

    @classmethod
    def get_supported_mimetype(cls, sres):
        if (cls is not ConsolidatorBase) and (sres["mimetype"] not in cls.supported_mimetypes):
            raise ValueError(f"A data source of {sres['mimetype']} type can not be handled by {cls.__name__}.")
        return sres["mimetype"]

    @property
    def shape(self) -> tuple[int, ...]:
        """Native shape of the data stored in assets

        This includes the leading (0th) dimension corresponding to the number of rows (if the join_method is stack)
        including skipped rows, if any. The number of relevant usable data rows may be lower, which is determined
        by the `seq_nums` field of StreamDatum documents."""

        if (self.join_method == "concat") and len(self.datum_shape) > 0:
            return self._num_rows * self.datum_shape[0], *self.datum_shape[1:]

        return self._num_rows, *self.datum_shape

    @property
    def chunks(self) -> tuple[tuple[int, ...], ...]:
        """Explicit (dask-style) specification of chunk sizes

        The produced chunk specification is a tuple of tuples of int that specify the sizes of each chunk in each
        dimension; it is based on the StreamResource parameter `chunk_shape`.

        If `chunk_shape` is an empty tuple -- assume the dataset is stored as a single chunk for all existing and
        new elements. Usually, however, `chunk_shape` is a tuple of int, in which case, we assume fixed-sized
        chunks with at most `chunk_shape[0]` elements (i.e. `_num_rows`); last chunk can be smaller. If chunk_shape
        is a tuple with less than `self.shape` elements -- assume it defines the chunk sizes along the leading
        dimensions.

        If the joining method is "concat", and `join_chunks = False`, the chunking along the leftmost dimensions
        is assumed to be preserved in each appended data point, i.e. consecutive chunks do not join, e.g. for a 1d
        array with chunks (3,3,1), the resulting chunking after 3 repeats is (3,3,1,3,3,1,3,3,1).
        When `join_chunks = True` (default), the chunk size along the leftmost dimension is determined by the
        chunk_shape parameter; this is the case when `join_method == "stack"` well.
        Chunking along the trailing dimensions is always preserved as in the original (single) array.
        """

        def list_summands(A: int, b: int, repeat: int = 1) -> tuple[int, ...]:
            # Generate a list with repeated b summing up to A; append the remainder if necessary
            # e.g. list_summands(13, 3) = [3, 3, 3, 3, 1]
            # if `repeat = n`, n > 1, copy and repeat the entire result n times
            return tuple([b] * (A // b) + ([A % b] if A % b > 0 else [])) * repeat or (0,)

        # If chunk shape is less than or equal to the total shape dimensions, chunk each specified dimension
        # starting from the leading dimension
        if len(self.chunk_shape) <= len(self.shape):
            if (
                self.join_method == "stack"
                or (self.join_method == "concat" and self.join_chunks)
                or len(self.chunk_shape) == 0
            ):
                result = tuple(
                    list_summands(ddim, cdim)
                    for ddim, cdim in zip(self.shape[: len(self.chunk_shape)], self.chunk_shape)
                )
            else:
                result = (
                    list_summands(self.datum_shape[0], self.chunk_shape[0], repeat=self._num_rows),
                    *[
                        list_summands(ddim, cdim)
                        for ddim, cdim in zip(self.shape[1 : len(self.chunk_shape)], self.chunk_shape[1:])  # noqa
                    ],
                )
            return result + tuple((d,) for d in self.shape[len(self.chunk_shape) :])  # noqa: E203

        # If chunk shape is longer than the total shape dimensions, raise an error
        else:
            raise ValueError(
                f"The shape of chunks, {self.chunk_shape}, should be less than or equal to the shape of data, "
                f"{self.shape}."
            )

    @property
    def has_skips(self) -> bool:
        """Indicates whether any rows should be skipped when mapping their indices to frame numbers

        This flag is intended to provide a shortcut for more efficient data access when there are no skips, and the
        mapping between indices and seq_nums is straightforward. In other case, the _seqnums_to_indices_map needs
        to be taken into account.
        """
        return self._num_rows > len(self._seqnums_to_indices_map)

    def adapter_parameters(self) -> dict:
        """A dictionary of parameters passed to an Adapter

        These parameters are intended to provide any additional information required to read a data source of a
        specific mimetype, e.g. "path" the path into an HDF5 file or "template" the filename pattern of a TIFF
        sequence.

        This method is to be subclassed as necessary.
        """
        return {}

    def structure(self) -> ArrayStructure:
        return ArrayStructure(
            data_type=self.data_type,
            shape=self.shape,
            chunks=self.chunks,
            dims=self.dims if self.dims else None,
        )

    def consume_stream_datum(self, doc: StreamDatum):
        """Process a new StreamDatum and update the internal data structure

        This will be called for every new StreamDatum received to account for the new added rows.
        This method _may need_ to be subclassed and expanded depending on a specific mimetype.
        Actions:
          - Parse the fields in a new StreamDatum
          - Increment the number of rows (implemented by the Base class)
          - Keep track of the correspondence between indices and seq_nums (implemented by the Base class)
          - Update the list of assets, including their uris, if necessary
          - Update shape and chunks
        """
        old_shape = self.shape  # Adding new rows updates self.shape
        self._num_rows += doc["indices"]["stop"] - doc["indices"]["start"]
        new_seqnums = range(doc["seq_nums"]["start"], doc["seq_nums"]["stop"])
        new_indices = range(doc["indices"]["start"], doc["indices"]["stop"])
        self._seqnums_to_indices_map.update(dict(zip(new_seqnums, new_indices)))
        return Patch(
            offset=(old_shape[0], *[0 for _ in self.shape[1:]]),
            shape=(self.shape[0] - old_shape[0], *self.shape[1:]),
        )

    def get_data_source(self) -> DataSource:
        """Return a DataSource object reflecting the current state of the streamed dataset.

        The returned DataSource is conceptually similar (and can be an instance of) tiled.structures.DataSource. In
        general, it describes associated Assets (filepaths, mimetype) along with their internal data structure
        (array shape, chunks, additional parameters) and should contain all information necessary to read the file.
        """
        return DataSource(
            mimetype=self.mimetype,
            assets=self.assets,
            structure_family=StructureFamily.array,
            structure=self.structure(),
            parameters=self.adapter_parameters(),
            management=Management.external,
        )

    def init_adapter(self, adapter_class=None):
        """Initialize a Tiled Adapter for reading the consolidated data

        Parameters
        ----------
        adapter_class : Optional[Type[Adapter]]
            An optional Adapter class to use for initialization; if not provided, the default adapter for the
            Consolidator's mimetype will be used.
        """

        adapter_class = adapter_class or DEFAULT_ADAPTERS_BY_MIMETYPE.get(self.mimetype)
        if not adapter_class:
            raise ValueError(f"No adapter found for mimetype {self.mimetype}")

        # Mimic the necessary aspects of a Tiled node with a namedtuple
        _Node = collections.namedtuple("Node", ["metadata_", "specs"])
        return adapter_class.from_catalog(self.get_data_source(), _Node({}, []), **self.adapter_parameters())

    def update_from_stream_resource(self, stream_resource: StreamResource):
        """Consume an additional related StreamResource document for the same data_key"""

        raise NotImplementedError("This method is not implemented in the base Consolidator class.")

    def validate(self, fix_errors=False) -> list[str]:
        """Validate the Consolidator's state against the expected structure"""

        # Initialize adapter from uris and determine the structure
        adapter_class = DEFAULT_ADAPTERS_BY_MIMETYPE[self.mimetype]
        uris = [asset.data_uri for asset in self.assets]
        structure = adapter_class.from_uris(*uris, **self.adapter_parameters()).structure()
        notes = []

        if self.shape != structure.shape:
            if not fix_errors:
                raise ValueError(f"Shape mismatch: {self.shape} != {structure.shape}")
            else:
                msg = f"Fixed shape mismatch: {self.shape} -> {structure.shape}"
                warnings.warn(msg, stacklevel=2)
                if self.join_method == "stack":
                    self._num_rows = structure.shape[0]
                    self.datum_shape = structure.shape[1:]
                elif self.join_method == "concat":
                    # Estimate the number of frames_per_event (multiplier)
                    multiplier = 1 if structure.shape[0] % structure.chunks[0][0] else structure.chunks[0][0]
                    self._num_rows = structure.shape[0] // multiplier
                    self.datum_shape = (multiplier,) + structure.shape[1:]
                notes.append(msg)

        if self.chunks != structure.chunks:
            if not fix_errors:
                raise ValueError(f"Chunk shape mismatch: {self.chunks} != {structure.chunks}")
            else:
                _chunk_shape = tuple(c[0] for c in structure.chunks)
                msg = f"Fixed chunk shape mismatch: {self.chunk_shape} -> {_chunk_shape}"
                warnings.warn(msg, stacklevel=2)
                self.chunk_shape = _chunk_shape
                notes.append(msg)

        if self.data_type != structure.data_type:
            if not fix_errors:
                raise ValueError(f"dtype mismatch: {self.data_type} != {structure.data_type}")
            else:
                msg = (
                    f"Fixed dtype mismatch: {self.data_type.to_numpy_dtype()} "
                    f"-> {structure.data_type.to_numpy_dtype()}"
                )
                warnings.warn(msg, stacklevel=2)
                self.data_type = structure.data_type
                notes.append(msg)

        if self.dims and (len(self.dims) != len(structure.shape)):
            if not fix_errors:
                raise ValueError(
                    f"Number of dimension names mismatch for a "
                    f"{len(structure.shape)}-dimensional array: {self.dims}"
                )
            else:
                old_dims = self.dims
                if len(old_dims) < len(structure.shape):
                    self.dims = (
                        ("time",)
                        + old_dims
                        + tuple(f"dim{i}" for i in range(len(old_dims) + 1, len(structure.shape)))
                    )
                else:
                    self.dims = old_dims[: len(structure.shape)]
                msg = f"Fixed dimension names: {old_dims} -> {self.dims}"
                warnings.warn(msg, stacklevel=2)
                notes.append(msg)

        assert self.init_adapter() is not None, "Adapter can not be initialized"

        return notes

    def get_adapter(self, adapters_by_mimetype=None):
        warnings.warn(
            f"{self.__class__.__name__}.get_adapter is deprecated and will be removed in a future release; "
            f"please, use {self.__class__.__name__}.init_adapter instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        adapter_class = (adapters_by_mimetype or {}).get(self.mimetype)
        return self.init_adapter(adapter_class=adapter_class)


class CSVConsolidator(ConsolidatorBase):
    supported_mimetypes: set[str] = {"text/csv;header=absent"}
    join_method: Literal["stack", "concat"] = "concat"
    join_chunks: bool = False

    def adapter_parameters(self) -> dict:
        allowed_keys = {
            "comment",
            "delimiter",
            "dtype",
            "encoding",
            "header",
            "names",
            "nrows",
            "sep",
            "skipfooter",
            "skiprows",
            "usecols",
        }
        return {k: v for k, v in {"header": None, **self._sres_parameters}.items() if k in allowed_keys}


class HDF5Consolidator(ConsolidatorBase):
    supported_mimetypes = {"application/x-hdf5"}

    def adapter_parameters(self) -> dict:
        """Parameters to be passed to the HDF5 adapter, a dictionary with the keys:

        dataset: list[str] - a path to the dataset within the hdf5 file represented as list split at `/`
        swmr: bool -- True to enable the single writer / multiple readers regime
        """
        params = {"dataset": self._sres_parameters["dataset"]}
        if slice := self._sres_parameters.get("slice", False):
            params["slice"] = slice
        if squeeze := self._sres_parameters.get("squeeze", False):
            params["squeeze"] = squeeze

        params["swmr"] = self._sres_parameters.get("swmr", True)
        params["locking"] = self._sres_parameters.get("locking", None)

        return params

    def update_from_stream_resource(self, stream_resource: StreamResource):
        """Add an Asset for a new StreamResource document"""
        if stream_resource["parameters"]["dataset"] != self._sres_parameters["dataset"]:
            raise ValueError("All StreamResource documents must have the same dataset path.")
        if stream_resource["parameters"].get("chunk_shape", ()) != self._sres_parameters.get("chunk_shape", ()):
            raise ValueError("All StreamResource documents must have the same chunk shape.")

        asset = Asset(
            data_uri=stream_resource["uri"], is_directory=False, parameter="data_uris", num=len(self.assets)
        )
        self.assets.append(asset)


class MultipartRelatedConsolidator(ConsolidatorBase):
    def __init__(
        self, permitted_extensions: set[str], stream_resource: StreamResource, descriptor: EventDescriptor
    ):
        super().__init__(stream_resource, descriptor)
        self.permitted_extensions: set[str] = permitted_extensions
        self.assets.clear()  # Assets will be populated based on datum indices
        self.data_uris: list[str] = []
        self.chunk_shape = self.chunk_shape or (1,)  # I.e. number of frames per file (tiff, jpeg, etc.)
        if self.join_method == "concat":
            assert self.datum_shape[0] % self.chunk_shape[0] == 0, (
                f"Number of frames per file ({self.chunk_shape[0]}) must divide the total number of frames per "
                f"datum ({self.datum_shape[0]}): variable-sized files are not allowed."
            )

        def int_replacer(match):
            """Normalize filename template

            Replace an integer format specifier with a new-style format specifier, i.e. convert the template string
            from "old" to "new" Python style, e.g. "%s%s_%06d.tif" to "filename_{:06d}.tif"

            """
            flags, width, precision, type_char = match.groups()

            # Handle the flags
            flag_str = ""
            if "-" in flags:
                flag_str = "<"  # Left-align
            if "+" in flags:
                flag_str += "+"  # Show positive sign
            elif " " in flags:
                flag_str += " "  # Space before positive numbers
            if "0" in flags:
                flag_str += "0"  # Zero padding

            # Build width and precision if they exist
            width_str = width if width else ""
            precision_str = f".{precision}" if precision else ""

            # Handle cases like "%6.6d", which should be converted to "{:06d}"
            if precision and width:
                flag_str = "0"
                precision_str = ""
                width_str = str(max(precision, width))

            # Construct the new-style format specifier
            return f"{{:{flag_str}{width_str}{precision_str}{type_char}}}"

        self.template = (
            self._sres_parameters["template"]
            .replace("%s", "{:s}", 1)
            .replace("%s", "")
            .replace("{:s}", self._sres_parameters.get("filename", ""), 1)
        )
        self.template = re.sub(r"%([-+#0 ]*)(\d+)?(?:\.(\d+))?([d])", int_replacer, self.template)

    def get_datum_uri(self, indx: int):
        """Return a full uri for a datum (an individual image file) based on its index in the sequence.

        This relies on the `template` parameter passed in the StreamResource, which is a string in the "new"
        Python formatting style that can be evaluated to a file name using the `.format(indx)` method given an
        integer index, e.g. "{:05d}.ext".

        If template is not set, we assume that the uri is provided directly in the StreamResource document (i.e.
        a single file case), and return it as is.
        """

        if self.template:
            assert os.path.splitext(self.template)[1] in self.permitted_extensions
            return self.uri + self.template.format(indx)
        else:
            return self.uri

    def consume_stream_datum(self, doc: StreamDatum):
        """Determine the number and names of files from indices of datums and the number of files per datum.

        In the most general case, each file may be a multipage tiff or a stack of images (frames) and a single
        datum may be composed of multiple such files, leading to a total of self.datum_shape[0] frames.
        Since each file necessarily represents a single chunk (tiffs can not be sub-chunked), the number of
        frames per file is equal to the leftmost chunk_shape dimension, self.chunk_shape[0].
        The number of files produced per each datum is then the ratio of these two numbers.

        If `join_method == "stack"`, we assume that each datum becomes its own index in the new leftmost dimension
        of the resulting dataset, and hence corresponds to a single file.
        """

        files_per_datum = self.datum_shape[0] // self.chunk_shape[0] if self.join_method == "concat" else 1
        first_file_indx = doc["indices"]["start"] * files_per_datum
        last_file_indx = doc["indices"]["stop"] * files_per_datum
        for indx in range(first_file_indx, last_file_indx):
            new_datum_uri = self.get_datum_uri(indx)
            new_asset = Asset(
                data_uri=new_datum_uri,
                is_directory=False,
                parameter="data_uris",
                num=len(self.assets) + 1,
            )
            self.assets.append(new_asset)
            self.data_uris.append(new_datum_uri)

        return super().consume_stream_datum(doc)


class TIFFConsolidator(MultipartRelatedConsolidator):
    supported_mimetypes = {"multipart/related;type=image/tiff"}

    def __init__(self, stream_resource: StreamResource, descriptor: EventDescriptor):
        super().__init__({".tif", ".tiff"}, stream_resource, descriptor)


class JPEGConsolidator(MultipartRelatedConsolidator):
    supported_mimetypes = {"multipart/related;type=image/jpeg"}

    def __init__(self, stream_resource: StreamResource, descriptor: EventDescriptor):
        super().__init__({".jpeg", ".jpg"}, stream_resource, descriptor)


class NPYConsolidator(MultipartRelatedConsolidator):
    supported_mimetypes = {"multipart/related;type=application/x-npy"}
    join_method: Literal["stack", "concat"] = "stack"

    # NOTE: NPYConsolidator is tailored for tests in databroker with ophyd.sim devices.
    # Use with caution in other settings!

    def __init__(self, stream_resource: StreamResource, descriptor: EventDescriptor):
        # Unlike other image sequence formats (e.g. TIFF) the filename
        # template is hard-coded in the NPY_SEQ handler. We inject it
        # here so that the rest of the processing can be handled
        # generically by ConsolidatorBase.
        stream_resource["parameters"]["template"] = "%s_%d.npy"
        data_key = stream_resource["data_key"]
        datum_shape = descriptor["data_keys"][data_key]["shape"]
        stream_resource["parameters"]["chunk_shape"] = (1, *datum_shape)
        super().__init__({".npy"}, stream_resource, descriptor)


CONSOLIDATOR_REGISTRY = collections.defaultdict(
    lambda: ConsolidatorBase,
    {
        "text/csv;header=absent": CSVConsolidator,
        "application/x-hdf5": HDF5Consolidator,
        "multipart/related;type=image/tiff": TIFFConsolidator,
        "multipart/related;type=image/jpeg": JPEGConsolidator,
        "multipart/related;type=application/x-npy": NPYConsolidator,
    },
)


def consolidator_factory(stream_resource_doc, descriptor_doc):
    consolidator_class = CONSOLIDATOR_REGISTRY[stream_resource_doc["mimetype"]]
    return consolidator_class(stream_resource_doc, descriptor_doc)
