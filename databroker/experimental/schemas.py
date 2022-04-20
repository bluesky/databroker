from typing import  Dict, Generic, List, Optional, TypeVar, Union
from enum import Enum

import pydantic
import pydantic.generics

from tiled.server.pydantic_array import ArrayStructure, ArrayMacroStructure, BuiltinDtype
from tiled.server.pydantic_dataframe import DataFrameStructure, DataFrameMacroStructure, DataFrameMicroStructure
from tiled.structures.core import StructureFamily
from tiled.structures.xarray import DataArrayStructure, DatasetStructure


import dask.array
import dask.dataframe
import numpy
import pandas


StrucT = TypeVar("StrucT")

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


# class Document(pydantic.generics.GenericModel, Generic[StrucT]):
class Document(pydantic.BaseModel):
    uid: str
    structure_family: StructureFamily
    structure: Union[ArrayStructure, DataFrameStructure]
    metadata: Dict
    specs: List[str]
    mimetype: str
    data_blob: Optional[bytes]
    data_url: Optional[pydantic.AnyUrl]

    @pydantic.root_validator
    def validate_structure_matches_structure_family(cls, values):
        # actual_structure_type = cls.__annotations__["structure"]  # this is what was filled in for StructureT
        actual_structure = values.get("structure")
        # Given the structure_family, we know what the structure type should be.
        expected_structure_type = structure_association[values.get("structure_family")]
        if values.get("expected_structure_type") == StructureFamily.node:
            raise Exception(f"{expected_structure_type} is not currently supported as a writable structure")
        elif not isinstance(actual_structure, expected_structure_type):
            raise Exception("The expected structure type does not match the received structure type")
        return values

    @pydantic.root_validator
    def check_data_source(cls, values):
        # Making them optional and setting default values might help to meet these conditions
        # with the current data types without getting any conflicts
        # if values.get('data_blob') is None and values.get('data_url') is None:
        #     raise ValueError("Not Valid: data_blob and data_url are both None. Use one of them")
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
    
    
# if __name__ == "__main__":
    # array = dask.array.from_array(numpy.ones((5, 5)))
    
    # name = 'TestNode'
    
    # structure_family = StructureFamily.array
    # structure = ArrayStructure(macro=ArrayMacroStructure(shape=array.shape, chunks=array.chunks),
    #                             micro=BuiltinDtype.from_numpy_dtype(array.dtype))
    
    # name = 'DataFrameNode'    
    # array = numpy.ones((5, 5))
    # data = {"Column1": array[0],
    #         "Column2": array[1],
    #         "Column3": array[2],
    #         "Column4": array[3],
    #         "Column5": array[4]}
    
    # df = pandas.DataFrame(data)
    # ddf = dask.dataframe.from_pandas(df, npartitions=len(df.columns))
    
    # meta={}
    # for key,value in df.items():
    #     meta[key] = value.dtypes.name
    
    # structure_family = StructureFamily.dataframe
    # # structure = DataFrameStructure(macro=DataFrameMacroStructure.from_dask_dataframe(ddf),
    # #                                 micro=DataFrameMicroStructure.from_dask_dataframe(ddf))
    # structure = DataFrameStructure( micro=DataFrameMicroStructure(meta=meta, divisions=ddf.divisions),
    #                                macro=DataFrameMacroStructure(npartitions=ddf.npartitions, columns=list(ddf.columns)))
    
    # meta = {'A': 0, 'B': 1}
    # metadata = {"columns": list(ddf.columns)}
    # specs=["BlueskyNode"]
    # # data_blob = b'1234'
    # #file:///a/b/c
    # # data_url = 'http://localhost:8000'
    # mimetype = 'image/png'
    # node = Document(uid=name, structure_family=structure_family, structure=structure,
    #                   metadata=meta, specs=specs, mimetype=mimetype)
