from typing import Dict, List, Optional, Union
from datetime import datetime

import pydantic
import pydantic.generics

from tiled.server.pydantic_array import ArrayStructure
from tiled.server.pydantic_dataframe import DataFrameStructure
from tiled.server.pydantic_sparse import SparseStructure
from tiled.structures.core import StructureFamily

from tiled.server.schemas import ReferenceDocument

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
    metadata: Dict
    specs: List[str]
    references: List[ReferenceDocument]
    updated_at: datetime


class Document(BaseDocument):
    structure_family: StructureFamily
    structure: Union[ArrayStructure, DataFrameStructure, SparseStructure]
    mimetype: str
    created_at: datetime
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

    @classmethod
    def from_document(cls, document, revision):
        return cls(
            key=document.key,
            metadata=document.metadata,
            specs=document.specs,
            references=document.references,
            updated_at=document.updated_at,
            revision=revision,
        )

    @classmethod
    def from_json(cls, json_doc):
        return cls(
            key=json_doc["key"],
            metadata=json_doc["metadata"],
            specs=json_doc["specs"],
            references=json_doc["references"],
            updated_at=json_doc["updated_at"],
            revision=json_doc["revision"],
        )
