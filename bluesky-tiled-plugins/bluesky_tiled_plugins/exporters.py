import copy
import json
from collections import defaultdict

import awkward
from tiled.utils import modules_available


async def json_exporter(adapter, metadata, filter_for_access):
    for spec in adapter.specs:
        if spec.name == "BlueskyRun" and spec.version.startswith("3."):
            break
    else:
        raise ValueError("This exporter only works with BlueskyRun v3.x")

    adapter = await filter_for_access(adapter, path_parts=[])
    yield json.dumps({"name": "start", "doc": metadata.get("start", {})})
    result = []

    # Generate descriptors
    stream_names = await (await adapter.lookup_adapter(["streams"])).keys_range(offset=0, limit=None)
    for desc_name in stream_names:
        desc_node = await adapter.lookup_adapter(["streams", desc_name])
        conf_node = await adapter.lookup_adapter(["configs", desc_name])
        desc_count = desc_node.metadata().get("desc_count", 1)  # Total number of descriptors for this stream
        part_names = set(await desc_node.keys_range(offset=0, limit=None))  # Composite parts
        if "internal" in part_names:
            part_names.remove("internal")
            internal_node = await desc_node.lookup_adapter(["internal"])
            columns = internal_node.structure().columns
            data_key_names = part_names.union(columns).difference(("internal",))
        else:
            internal_node = None
            data_key_names = part_names

        # Assemble dictionaries of data keys and configuration keys
        data_keys, conf_list = {}, [defaultdict(dict) for _ in range(desc_count)]
        object_keys = defaultdict(list)
        conf_recs = awkward.from_buffers(
            conf_node.structure().form, conf_node.structure().length, await conf_node.read()
        ).to_list()
        for item in conf_recs:
            data_key = item.pop("data_key")  # Must be present
            desc_indx = item.pop("desc_indx", 0)
            value = item.pop("value", None)
            timestamp = item.pop("timestamp", None)
            if data_key in data_key_names:
                # This is a proper data_key for internal or external data
                data_keys[data_key] = {k: v for k, v in item.items() if v is not None}
                object_name = item.get("object_name")
                object_keys[object_name].append(data_key)
            elif value is not None:
                # This is a configuration data_key
                object_name = item.pop("object_name")

                conf_data = conf_list[desc_indx][object_name].get("data", {})
                conf_timestamps = conf_list[desc_indx][object_name].get("timestamps", {})
                conf_data_keys = conf_list[desc_indx][object_name].get("data_keys", {})

                conf_data[data_key] = value
                conf_timestamps[data_key] = timestamp
                conf_data_keys[data_key] = {k: v for k, v in item.items() if v is not None}

                conf_list[desc_indx][object_name]["data"] = conf_data
                conf_list[desc_indx][object_name]["timestamps"] = conf_timestamps
                conf_list[desc_indx][object_name]["data_keys"] = conf_data_keys

        # Fixed part of the descriptor
        desc_doc = desc_node.metadata().get("extra", {})
        desc_doc["run_start"] = metadata.get("start", {}).get("uid")
        desc_doc["name"] = desc_name
        desc_doc["data_keys"] = data_keys
        desc_doc["object_keys"] = object_keys

        # Variable part of the descriptor
        desc_time_uids = conf_node.metadata()["descriptors"]
        for desc_indx in range(desc_count):
            desc_doc["uid"] = desc_time_uids[desc_indx]["uid"]
            desc_doc["time"] = desc_time_uids[desc_indx]["time"]
            desc_doc["configuration"] = conf_list[desc_indx]

            result.append({"name": "descriptor", "doc": copy.deepcopy(desc_doc)})

        # Generate events
        if internal_node:

            def format_value(value, precision=6):
                if hasattr(value, "__array__"):
                    return [format_value(v) for v in value]
                elif isinstance(value, float):
                    if (precision is None) and (value % 1 > 1e-6):
                        # Do not force "sizable" float to int
                        return value
                    return round(value, precision)

            df = await internal_node.read()
            keys = [k for k in df.columns if k not in {"seq_num", "time"} and not k.startswith("ts_")]
            for row in df.to_dict(orient="records"):
                desc_uid = desc_time_uids[0]["uid"]  # same as desc_node.metadata()["uid"]
                for _desc_uid_time in desc_time_uids[1:]:
                    if _desc_uid_time["time"] <= row["time"]:
                        desc_uid = _desc_uid_time["uid"]
                event_doc = {"seq_num": row["seq_num"], "time": row["time"]}
                event_doc["uid"] = f"event-{desc_uid}-{row['seq_num']}"  # can be anything (unique)
                event_doc["descriptor"] = desc_uid
                event_doc["data"] = {k: format_value(row[k], data_keys[k].get("precision")) for k in keys}
                event_doc["timestamps"] = {k: format_value(row[f"ts_{k}"], precision=6) for k in keys}
                result.append({"name": "event", "doc": event_doc})

        # Generate Stream Resources and Datums
        # TODO: needs thorough testing, incl. cases with multiple hdf5 files
        desc_uid = desc_node.metadata()["uid"]
        for data_key in part_names:
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
            datum_shape = desc_node.metadata()[data_key]["shape"]

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
    if modules_available("databroker"):
        from databroker.mongo_normalized import batch_documents

        result = [
            {"name": x[0], "doc": x[1]}
            for x in batch_documents([(y["name"], y["doc"]) for y in result], size=1000)
        ]

    for doc in result:
        yield "\n" + json.dumps(doc)

    yield "\n" + json.dumps({"name": "stop", "doc": metadata.get("stop", {})})
