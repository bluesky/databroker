import json
import msgpack
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Security
import pydantic
from tiled.server.core import PatchedStreamingResponse
from tiled.server.dependencies import entry


class NameDocumentPair(pydantic.BaseModel):
    name: str  # TODO Lock this down to an enum of the document types.
    document: dict


router = APIRouter()


@router.get("/documents/{path:path}", response_model=NameDocumentPair)
@router.get("/documents", response_model=NameDocumentPair, include_in_schema=False)
def documents(
    request: Request,
    fill: Optional[bool] = False,
    run=Security(entry, scopes=["read:data", "read:metadata"]),
):
    # Check that this is a BlueskyRun.
    if not hasattr(run, "documents"):
        raise HTTPException(status_code=404, detail="This is not a BlueskyRun.")
    DEFAULT_MEDIA_TYPE = "application/json"
    media_types = request.headers.get("Accept", DEFAULT_MEDIA_TYPE).split(", ")
    for media_type in media_types:
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPE
        if media_type == "application/x-msgpack":
            # (name, doc) pairs as msgpack

            def generator_func():
                packer = msgpack.Packer()
                for item in run.documents(fill=fill):
                    yield packer.pack(item)

            generator = generator_func()
            return PatchedStreamingResponse(
                generator, media_type="application/x-msgpack"
            )
        if media_type == "application/json":
            # (name, doc) pairs as newline-delimited JSON
            generator = (json.dumps(item) + "\n" for item in run.documents(fill=fill))
            return PatchedStreamingResponse(
                generator, media_type="application/x-ndjson"
            )
    else:
        raise HTTPException(
            status_code=406,
            detail=", ".join(["application/json", "application/x-msgpack"]),
        )
