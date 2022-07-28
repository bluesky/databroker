from typing import Dict, List, Optional, Union
from datetime import datetime, timezone

import pydantic
import pydantic.generics

from tiled.server.pydantic_array import ArrayStructure
from tiled.server.pydantic_dataframe import DataFrameStructure
from tiled.server.pydantic_sparse import SparseStructure
from tiled.structures.core import StructureFamily


# Map structure family to the associated
# structure model. This is used by the validator.
structure_association = {
    StructureFamily.array: ArrayStructure,
    StructureFamily.dataframe: DataFrameStructure,
    StructureFamily.sparse: SparseStructure,
    # StructureFamily.node
    # ...
}


class BaseDocument(pydantic.BaseModel):
    key: str
    structure_family: StructureFamily
    structure: Union[ArrayStructure, DataFrameStructure, SparseStructure]
    metadata: Dict
    specs: List[str]
    mimetype: str
    created_at: datetime
    updated_at: datetime

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


class Document(BaseDocument):
    data_blob: Optional[bytes]
    data_url: Optional[pydantic.AnyUrl]

    @pydantic.root_validator
    def check_data_source(cls, values):
        # Making them optional and setting default values might help to meet these conditions
        # with the current data types without getting any conflicts
        if values.get("data_blob") is not None and values.get("data_url") is not None:
            raise ValueError(
                "Not Valid: data_blob and data_url contain values. Use just one"
            )
        return values


class DocumentRevision(BaseDocument):
    revision: int

    # def __init__(self, document:Document):
    #     self.key = document.key
    #     self.structure_family = document.structure_family
    #     self.structure = document.structure
    #     self.specs = document.specs
    #     self.mimetype = document.mimetype
    #     self.created_at = document.created_at

    #     # self.__dict__ = document.__dict__.copy()
    #     self.updated_at = datetime.now(tz=timezone.utc)
    #     if document.data_blob() is not None:
    #         self.data_blob = None
    #     elif document.data_url is not None:
    #         self.data_url = document.data_url


if __name__ == "__main__":

    import numpy
    import time
    from tiled.server.pydantic_array import ArrayMacroStructure, BuiltinDtype

    array = numpy.ones((5, 5))

    key = "ABC"
    structure_family = StructureFamily.array
    structure = ArrayStructure(
        macro=ArrayMacroStructure(
            shape=array.shape,
            # just one chunk for now...
            chunks=tuple((size,) for size in array.shape),
        ),
        micro=BuiltinDtype.from_numpy_dtype(array.dtype),
    )
    metadata = {"symbol": "C", "edge": "K"}
    specs = ["BlueskyNode"]
    mimetype = "application/x-hdf5"
    created_at = datetime.now(tz=timezone.utc)
    data_url = "http://www.gmail.com"

    doc = Document(
        key=key,
        structure_family=structure_family,
        structure=structure,
        metadata=metadata,
        specs=specs,
        mimetype=mimetype,
        created_at=created_at,
        updated_at=created_at,
        data_url=data_url,
    )

    time.sleep(5)

    updated_at = datetime.now(tz=timezone.utc)
    child_doc = DocumentRevision(
        key=doc.key,
        structure_family=doc.structure_family,
        structure=doc.structure,
        metadata=doc.metadata,
        specs=doc.specs,
        mimetype=doc.mimetype,
        created_at=doc.created_at,
        data_url=doc.data_url,
        revision=0,
        updated_at=updated_at,
    )
