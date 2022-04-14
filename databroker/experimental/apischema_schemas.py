from apischema import ValidationError, deserialize, validator

from dataclasses import dataclass
from typing import Dict, Generic, List, Optional, TypeVar

from tiled.structures.core import StructureFamily
# from tiled.structures.array import ArrayStructure, ArrayMacroStructure, BuiltinDtype
from tiled.structures.dataframe import DataFrameStructure
from tiled.structures.xarray import DataArrayStructure, DatasetStructure
from tiled.server.pydantic_array import ArrayStructure, ArrayMacroStructure, BuiltinDtype

from pathlib import Path

import dask.array
import numpy
import re

# import pytest



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

# @dataclass
# class Box(Generic[T]):
    # content: T
    
@dataclass
class Document(Generic[StrucT]):
    id: str
    structure_family: StructureFamily
    structure: StrucT
    metadata: Dict
    specs: List[str]
    mimetype: str
    data_blob: Optional[bytes] = None
    data_url: Optional[str] = None
    
    def __post_init__(self):
        # if self.data_blob is None and self.data_url is None:
        #     raise ValidationError("Not Valid: data_blob and data_url are both None. Use one of them")
        if self.data_blob is not None and self.data_url is not None:
            raise ValidationError("Not Valid: data_blob and data_url contain values. Use just one")
            
    @validator
    def is_mime_type(self):
        m_type, _, _ = self.mimetype.partition('/')
        mime_type_list = set(['application', 'audio', 'font', 'example', 'image', 'message', 'model', 'multipart', 'text', 'video'])
        if m_type not in mime_type_list:
            raise ValidationError(f"{m_type} is not a valid mime type")
    
    @validator
    def structure_matches_structure_family(self):
        # actual_structure_type = cls.__annotations__["structure"]  # this is what was filled in for StructureT
        actual_structure_type = self.structure
        # Given the structure_family, we know what the structure type should be.
        expected_structure_type = structure_association[self.structure_family]
        # print(actual_structure_type, expected_structure_type)
        if self.structure_family == StructureFamily.node:
            raise ValidationError(f"{actual_structure_type} is not currently supported as a writable structure")
        elif not isinstance(actual_structure_type, expected_structure_type):
            raise ValidationError("The expected structure type does not match the received structure type")
    
    @validator
    def is_url_valid(self):
    
        regex = re.compile( 
            r"(?:http://(?:(?:(?:(?:(?:[a-zA-Z\d](?:(?:[a-zA-Z\d]|-)*[a-zA-Z\d])?)\.)*(?:[a-zA-Z](?:(?:[a-zA-Z\d]|-)*[a-zA-Z\d])?))|"
            r"(?:(?:\d+)(?:\.(?:\d+)){3}))(?::(?:\d+))?)(?:/(?:(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|[;:@&=])*)"
            r"(?:/(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|[;:@&=])*))*)(?:\?(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|"
            r"(?:%[a-fA-F\d]{2}))|[;:@&=])*))?)?)|(?:ftp://(?:(?:(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|[;?&=])*)"
            r"(?::(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|[;?&=])*))?@)?(?:(?:(?:(?:(?:[a-zA-Z\d](?:(?:[a-zA-Z\d]|-)"
            r"*[a-zA-Z\d])?)\.)*(?:[a-zA-Z](?:(?:[a-zA-Z\d]|-)*[a-zA-Z\d])?))|(?:(?:\d+)(?:\.(?:\d+)){3}))(?::(?:\d+))?))"
            r"(?:/(?:(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|[?:@&=])*)(?:/(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|"
            r"[?:@&=])*))*)(?:;type=[AIDaid])?)?)|(?:file://(?:(?:(?:(?:(?:[a-zA-Z\d](?:(?:[a-zA-Z\d]|-)*[a-zA-Z\d])?)\.)*(?:[a-zA-Z]"
            r"(?:(?:[a-zA-Z\d]|-)*[a-zA-Z\d])?))|(?:(?:\d+)(?:\.(?:\d+)){3}))|localhost)?/(?:(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|"
            r"(?:%[a-fA-F\d]{2}))|[?:@&=])*)(?:/(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|[?:@&=])*))*))|"
            r"(?:https://(?:(?:(?:(?:(?:[a-zA-Z\d](?:(?:[a-zA-Z\d]|-)*[a-zA-Z\d])?)\.)*(?:[a-zA-Z](?:(?:[a-zA-Z\d]|-)*[a-zA-Z\d])?))|"
            r"(?:(?:\d+)(?:\.(?:\d+)){3}))(?::(?:\d+))?)(?:/(?:(?:(?:(?:[a-zA-Z\d$\-.+!*'(),]|(?:%[a-fA-F\d]{2}))|[;:@&=])*)"
            r"(?:/(?:(?:(?:[a-zA-Z\d$\-.+!*'(),]|(?:%[a-fA-F\d]{2}))|[;:@&=])*))*)(?:\?(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|"
            r"[;:@&=])*))?)?)|(?:ftps://(?:(?:(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|[;?&=])*)(?::(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|"
            r"(?:%[a-fA-F\d]{2}))|[;?&=])*))?@)?(?:(?:(?:(?:(?:[a-zA-Z\d](?:(?:[a-zA-Z\d]|-)*[a-zA-Z\d])?)\.)*(?:[a-zA-Z](?:(?:[a-zA-Z\d]|-)"
            r"*[a-zA-Z\d])?))|(?:(?:\d+)(?:\.(?:\d+)){3}))(?::(?:\d+))?))(?:/(?:(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|"
            r"[?:@&=])*)(?:/(?:(?:(?:[a-zA-Z\d$\-_.+!*'(),]|(?:%[a-fA-F\d]{2}))|[?:@&=])*))*)(?:;type=[AIDaid])?)?)",
            re.IGNORECASE)
        if not regex.search(self.data_url):
            raise ValidationError("the link provided in data_url is not a compatible url")

if __name__ == "__main__":
    
    array = dask.array.from_array(numpy.ones((5, 5)))
    doc = deserialize(Document, {"id": "Node",
                              "structure_family": StructureFamily.array,
                              "structure": ArrayStructure(
                                  macro=ArrayMacroStructure(shape=array.shape, chunks=array.chunks),
                                  micro=BuiltinDtype.from_numpy_dtype(array.dtype)),
                              "metadata": {"element": "Cr"},
                              "specs": ["BlueskyNode"], "mimetype": "image/png",
                              "data_blob": b"1234"}, pass_through={bytes})
    