import json
import msgpack
from typing import Optional
from jsonschema import ValidationError

from event_model import DocumentNames, schema_validators
from fastapi import APIRouter, Depends, HTTPException, Request, Security
import pydantic
from starlette.responses import StreamingResponse
from tiled.server.authentication import check_scopes, get_current_principal, get_current_scopes, get_session_state
from tiled.server.dependencies import get_entry, get_root_tree
from tiled.type_aliases import Scopes


class NamedDocument(pydantic.BaseModel):
    name: DocumentNames
    doc: dict


router = APIRouter()


@router.get("/documents/{path:path}", response_model=NamedDocument)
@router.get("/documents", response_model=NamedDocument, include_in_schema=False)
async def get_documents(
    request: Request,
    path: str,
    principal=Depends(get_current_principal),
    root_tree=Depends(get_root_tree),
    session_state: dict = Depends(get_session_state),
    authn_scopes: Scopes = Depends(get_current_scopes),
    fill: Optional[bool] = False,
    _=Security(check_scopes, scopes=["read:data", "read:metadata"])
):

    from .mongo_normalized import BlueskyRun

    run = await get_entry(
        path,
        ["read:data", "read:metadata"],
        principal,
        authn_scopes,
        root_tree,
        session_state,
        request.state.metrics,
        None,
        getattr(request.app.state, "access_policy", None),
    )

    if not isinstance(run, BlueskyRun):
        raise HTTPException(status_code=404, detail="This is not a BlueskyRun.")
    DEFAULT_MEDIA_TYPE = "application/json-seq"
    media_types = request.headers.get("Accept", DEFAULT_MEDIA_TYPE).split(", ")
    for media_type in media_types:
        if media_type == "*/*":
            media_type = DEFAULT_MEDIA_TYPE
        if media_type == "application/x-msgpack":
            # (name, doc) pairs as msgpack

            def generator_func():
                packer = msgpack.Packer()
                for name, doc in run.documents(fill=fill):
                    yield packer.pack({"name": name, "doc": doc})

            generator = generator_func()
            return StreamingResponse(
                generator, media_type="application/x-msgpack"
            )
        if media_type == "application/json-seq":
            # (name, doc) pairs as newline-delimited JSON
            generator = (json.dumps({"name": name, "doc": doc}) + "\n" for name, doc in run.documents(fill=fill))
            return StreamingResponse(
                generator, media_type="application/json-seq"
            )
    else:
        raise HTTPException(
            status_code=406,
            detail=", ".join(["application/json-seq", "application/x-msgpack"]),
        )


@router.post("/documents/{path:path}")
@router.post("/documents", include_in_schema=False)
async def post_documents(
    request: Request,
    named_doc: NamedDocument,
    path: str,
    principal=Depends(get_current_principal),
    root_tree=Depends(get_root_tree),
    session_state: dict = Depends(get_session_state),
    authn_scopes: Scopes = Depends(get_current_scopes),
    fill: Optional[bool] = False,
    _=Security(check_scopes, scopes=["write:data", "write:metadata"])
):
    from .mongo_normalized import MongoAdapter

    catalog = await get_entry(
        path,
        ["write:data", "write:metadata"],
        principal,
        authn_scopes,
        root_tree,
        session_state,
        request.state.metrics,
        None,
        getattr(request.app.state, "access_policy", None),
    )

    # Check that this is a BlueskyRun.
    if not isinstance(catalog, MongoAdapter):
        raise HTTPException(status_code=404, detail="This is not a CatalogOfBlueskyRuns.")
    serializer = catalog.get_serializer()
    try:
        schema_validators[named_doc.name].validate(named_doc.doc)
    except ValidationError as err:
        raise HTTPException(status_code=400, detail=err.message)
    serializer(named_doc.name.value, named_doc.doc)
