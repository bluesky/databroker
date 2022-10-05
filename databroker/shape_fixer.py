"""
Given a Run, fill one Event from each stream, measure its shape, and patch the
Descriptors in that stream.
"""
from event_model import Filler


def log(*args, logfile, progress):
    progress.write(" ".join([str(arg) for arg in args]))
    print(*args, file=logfile, flush=True)


def measure(
    mds_database,
    asset_database,
    descriptor,
    root_map,
    handler_registry,
    patch_resource=None,
):
    """
    Return {data_key: correct shape} for all external data_keys
    """
    recorded_shapes = {}
    for key, data_key in descriptor["data_keys"].items():
        if data_key.get("external"):
            recorded_shapes[key] = data_key["shape"]
    if not recorded_shapes:
        # No external data, nothing to measure
        return {}, {}
    filler = Filler(handler_registry=handler_registry, inplace=False, root_map=root_map)
    datum_collection = asset_database["datum"]
    resource_collection = asset_database["resource"]
    cursor = mds_database["event"].find(
        {"descriptor": descriptor["uid"]}, sort=[("time", 1)]
    )
    try:
        event = next(cursor)
        event["filled"] = {key: False for key in recorded_shapes}
    except StopIteration:
        # No Events, nothing to measure
        return {}, {}
    filler("descriptor", descriptor)
    resources = set()
    measured_shapes = {}
    for key in recorded_shapes:
        datum = datum_collection.find_one({"datum_id": event["data"][key]})
        if datum["resource"] not in resources:
            resource = resource_collection.find_one({"uid": datum["resource"]})
            if patch_resource is not None:
                resource = patch_resource(resource)
            filler("resource", resource)
        filler("datum", datum)
    _, filled_event = filler("event", event)
    for key in recorded_shapes:
        data = filled_event["data"][key]
        measured_shapes[key] = [int(dim) for dim in data.shape]
    return recorded_shapes, measured_shapes


def fix(mds_database, descriptor, measured_shapes):
    for key, measured_shape in measured_shapes.items():
        mds_database["event_descriptor"].update_one(
            {"uid": descriptor["uid"]},
            {"$set": {f"data_keys.{key}.shape": measured_shape}},
            upsert=False,
        )
