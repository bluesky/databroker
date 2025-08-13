import copy
import json
from collections import defaultdict


async def json_seq_exporter(mimetype, adapter, metadata, filter_for_access):
    """
    Export BlueskyRun is newline-delimited sequence of JSON.

    Format is like:

    {"name": "start", "doc": {...}}
    {"name": "descriptor", "doc": {...}}
    ...
    """
    for spec in adapter.specs:
        if spec.name == "BlueskyRun" and spec.version.startswith("3."):
            break
    else:
        raise ValueError("This exporter only works with BlueskyRun v3.x")

    adapter = await filter_for_access(adapter)
    yield json.dumps({"name": "start", "doc": metadata.get("start", {})})
    result = []

    # Generate descriptors
    stream_names = await (await adapter.lookup_adapter(["streams"])).keys_range(offset=0, limit=None)
    for desc_name in stream_names:
        desc_node = await adapter.lookup_adapter(["streams", desc_name])
        desc_meta = desc_node.metadata()
        part_names = set(await desc_node.keys_range(offset=0, limit=None))  # Composite parts

        # First (or the only) descriptor
        desc_doc = {k: v for k, v in desc_meta.items() if k not in {"_config_updates"}}
        desc_doc["run_start"] = metadata.get("start", {}).get("uid")
        desc_doc["name"] = desc_name
        desc_doc["object_keys"] = defaultdict(list)
        for key, val in desc_doc["data_keys"].items():
            if obj_name := val.get("object_name"):
                desc_doc["object_keys"][obj_name].append(key)

        result.append({"name": "descriptor", "doc": desc_doc})

        # Process subsequent descriptors, if any
        desc_time_uids = [{"uid": desc_doc["uid"], "time": desc_doc["time"]}]
        for upd in desc_meta.get("_config_updates", []):
            desc_doc = copy.deepcopy(desc_doc)
            desc_doc["uid"] = upd["uid"]
            desc_doc["time"] = upd["time"]
            desc_time_uids.extend([{"uid": desc_doc["uid"], "time": desc_doc["time"]}])
            for obj_name, obj in upd.get("configuration", {}).items():
                # This assumes that that the full configuration was present in the first descriptor
                for key in obj["data"].keys():
                    desc_doc["configuration"][obj_name]["data"][key] = obj["data"][key]
                    desc_doc["configuration"][obj_name]["timestamps"][key] = obj["timestamps"][key]

            result.append({"name": "descriptor", "doc": desc_doc})

        # Generate events
        if "internal" in part_names:
            internal_node = await desc_node.lookup_adapter(["internal"])
            df = await internal_node.read()
            keys = [k for k in df.columns if k not in {"seq_num", "time"} and not k.startswith("ts_")]
            for row in df.to_dict(orient="records"):
                desc_uid = desc_time_uids[0]["uid"]  # same as desc_node.metadata()["uid"] if no updates
                for _desc_uid_time in desc_time_uids[1:]:
                    if _desc_uid_time["time"] <= row["time"]:
                        desc_uid = _desc_uid_time["uid"]
                event_doc = {"seq_num": row["seq_num"], "time": row["time"]}
                event_doc["uid"] = f"event-{desc_uid}-{row['seq_num']}"  # can be anything (unique)
                event_doc["descriptor"] = desc_uid
                event_doc["data"] = {k: row[k].tolist() if hasattr(row[k], "__array__") else row[k] for k in keys}
                event_doc["timestamps"] = {k: row[f"ts_{k}"] for k in keys}
                result.append({"name": "event", "doc": event_doc})

        # Generate Stream Resources and Datums
        desc_uid = desc_node.metadata()["uid"]
        for data_key in part_names.difference(("internal",)):
            # Loop over data_keys for external data only
            sres_uid = f"sr-{desc_uid}-{data_key}"  # can be anything (unique)
            ds = (await desc_node.lookup_adapter([data_key])).data_sources[0]
            uri = ds.assets[0].data_uri
            for ast in ds.assets:
                if ast.parameter in {"data_uris", "data_uri"}:
                    uri = ast.data_uri
                    break
            sres_doc = {
                "data_key": data_key,
                "uid": sres_uid,
                "run_start": metadata.get("start", {}).get("uid"),
                "mimetype": ds.mimetype,
                "parameters": ds.parameters,
                "uri": uri,
            }
            result.append({"name": "stream_resource", "doc": sres_doc})

            # Generate a single stream_datum document for the entire stream
            sdat_uid = f"sd-{desc_uid}-{data_key}-0"  # can be anything (unique)
            total_shape = ds.structure.shape
            datum_shape = desc_node.metadata()["data_keys"][data_key]["shape"]

            max_indx = (
                total_shape[0] // datum_shape[0] - 1
                if len(total_shape) == len(datum_shape)
                else total_shape[0] - 1
            )
            sdat_doc = {
                "uid": sdat_uid,
                "stream_resource": sres_uid,
                "descriptor": desc_uid,
                "indices": {"start": 0, "stop": max_indx},
                "seq_nums": {"start": 1, "stop": max_indx + 1},
            }
            result.append({"name": "stream_datum", "doc": sdat_doc})

    # Make sure that the order of documents is (approximately) correct
    result = sorted(
        result,
        key=lambda x: (
            x["doc"].get("time", float("inf")),
            {"stream_resource": 0, "stream_datum": 1}.get(x["name"]),
        ),
    )

    # Combine events into event_pages
    #     if modules_available("databroker"):
    #         from databroker.mongo_normalized import batch_documents
    #
    #         result = [
    #             {"name": x[0], "doc": x[1]}
    #             for x in batch_documents([(y["name"], y["doc"]) for y in result], size=1000)
    #         ]

    for doc in result:
        yield "\n" + json.dumps(doc)

    yield "\n" + json.dumps({"name": "stop", "doc": metadata.get("stop", {})})
