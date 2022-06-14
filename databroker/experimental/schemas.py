from typing import Dict, List, Optional, Union

import pydantic
import pydantic.generics

from tiled.server.pydantic_array import ArrayStructure
from tiled.server.pydantic_dataframe import DataFrameStructure
from tiled.server.pydantic_xarray import DataArrayStructure, DatasetStructure
from tiled.structures.core import StructureFamily


# Map structure family to the associated
# structure model. This is used by the validator.
structure_association = {
    StructureFamily.array: ArrayStructure,
    StructureFamily.dataframe: DataFrameStructure,
    StructureFamily.xarray_data_array: DataArrayStructure,
    StructureFamily.xarray_dataset: DatasetStructure,
    # StructureFamily.mode?????
    # ...
}


class Document(pydantic.BaseModel):
    key: str
    structure_family: StructureFamily
    structure: Union[ArrayStructure, DataFrameStructure, DataArrayStructure]
    metadata: Dict
    specs: List[str]
    mimetype: str
    data_blob: Optional[bytes]
    data_url: Optional[pydantic.AnyUrl]

    @pydantic.root_validator
    def validate_structure_matches_structure_family(cls, values):
        actual_structure = values.get("structure")
        # Given the structure_family, we know what the structure type should be.
        expected_structure_type = structure_association[values.get("structure_family")]
        if expected_structure_type == StructureFamily.node:
            raise Exception(
                f"{expected_structure_type} is not currently supported as a writable structure"
            )
        elif not isinstance(actual_structure, expected_structure_type):
            raise Exception(
                "The expected structure type does not match the received structure type"
            )
        return values

    @pydantic.root_validator
    def check_data_source(cls, values):
        # Making them optional and setting default values might help to meet these conditions
        # with the current data types without getting any conflicts
        if values.get("data_blob") is not None and values.get("data_url") is not None:
            raise ValueError(
                "Not Valid: data_blob and data_url contain values. Use just one"
            )
        return values

    @pydantic.validator("mimetype")
    def is_mime_type(cls, v):
        m_type, _, _ = v.partition("/")
        mime_type_list = set(
            [
                "application",
                "audio",
                "font",
                "example",
                "image",
                "message",
                "model",
                "multipart",
                "text",
                "video",
            ]
        )

        if m_type not in mime_type_list:
            raise ValueError(f"{m_type} is not a valid mime type")
        return v


if __name__ == "__main__":
    import dask.array
    import numpy
    import xarray

    from dataclasses import asdict
    from tiled.adapters.xarray import DataArrayAdapter

    array = numpy.random.random((10, 10))
    xarr_adapter = DataArrayAdapter.from_data_array(
        xarray.DataArray(
            xarray.Variable(
                data=dask.array.from_array(array),
                dims=["x", "y"],
                attrs={"thing": "stuff"},
            ),
            coords={
                "x": dask.array.arange(len(array)),
                "y": 10 * dask.array.arange(len(array)),
            },
        )
    )

    xarr_macro = asdict(xarr_adapter.macrostructure())
    # uid = "123456"
    # structure_family = StructureFamily.xarray_data_array
    structure = DataArrayStructure.from_json(
        {"macro": xarr_macro, "micro": xarr_adapter.microstructure()}
    )
    # metadata = {"a": 1, "b": 2}
    # specs = ["BlueskyNode"]
    # mimetype = "application"
