import base64
import time
from dataclasses import asdict

from io import BytesIO

from tiled.client.utils import handle_error
from tiled.structures.array import ArrayMacroStructure, ArrayStructure, BuiltinDtype
from tiled.structures.dataframe import (
    DataFrameStructure,
    DataFrameMacroStructure,
    DataFrameMicroStructure,
)
from tiled.structures.core import StructureFamily
from tiled.structures.dataframe import serialize_arrow


def submit_array(context, array, metadata, specs, mimetype):
    structure = ArrayStructure(
        macro=ArrayMacroStructure(
            shape=array.shape,
            # just one chunk for now...
            chunks=tuple((size,) for size in array.shape),
        ),
        micro=BuiltinDtype.from_numpy_dtype(array.dtype),
    )
    data = {
        "metadata": metadata,
        "structure": asdict(structure),
        "structure_family": StructureFamily.array,
        "specs": specs,
        "mimetype": mimetype,
    }
    document = context.post_json("/node/metadata/", data)
    uid = document["uid"]
    data_response = context._client.put(f"/array/full/{uid}", content=array.tobytes())
    handle_error(data_response)


def submit_dataframe(context, dataframe, metadata, specs, mimetype):
    from dask.dataframe.utils import make_meta

    structure = DataFrameStructure(
        micro=DataFrameMicroStructure(meta=make_meta(dataframe), divisions=[]),
        macro=DataFrameMacroStructure(npartitions=1, columns=list(dataframe.columns)),
    )

    data = {
        "metadata": metadata,
        "structure": asdict(structure),
        "structure_family": StructureFamily.dataframe,
        "specs": specs,
        "mimetype": mimetype,
    }

    data["structure"]["micro"]["meta"] = base64.b64encode(
        bytes(serialize_arrow(data["structure"]["micro"]["meta"], {}))
    ).decode()

    document = context.post_json("/node/metadata/", data)
    uid = document["uid"]

    data_response = context._client.put(
        f"/dataframe/full/{uid}", content=bytes(serialize_arrow(dataframe, {}))
    )
    handle_error(data_response)
