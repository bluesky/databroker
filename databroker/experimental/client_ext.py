import time
from dataclasses import asdict

from tiled.client.utils import handle_error
from tiled.structures.array import ArrayMacroStructure, ArrayStructure, BuiltinDtype
from tiled.structures.core import StructureFamily


def submit_recon(context, array, metadata, specs, mimetype):
    structure = ArrayStructure(
        macro=ArrayMacroStructure(
            shape=array.shape,
            # just one chunk for now...
            chunks=tuple((size,) for size in array.shape),
        ),
        micro=BuiltinDtype.from_numpy_dtype(array.dtype),
    )
    
    data = {"metadata": metadata, "structure": asdict(structure), "structure_family": StructureFamily.array,
            "specs": specs, "mimetype": mimetype}    
    response = context._client.post("/node/metadata/", json=data)
    handle_error(response)
    uid = response.json()["uid"]
    # time.sleep(0.1)
    # context._client.put(f"/node/metadata/{uid}", content=array.tobytes())
