from pytest import fixture
import pytest
import event_model
import numpy as np
import time
from databroker.in_memory import BlueskyInMemoryCatalog

data_shape = (1000, 1000)


@fixture(params=[(('primary', 'baseline'), True), (('primary', 'primary'), True),
                 (('primary', 'baseline'), False), (('primary', 'primary'), False)])
def multi_descriptor_doc_stream(request):
    streams, with_dims = request.param

    def doc_gen(stream_names):

        # Compose run start
        run_bundle = event_model.compose_run()  # type: event_model.ComposeRunBundle
        start_doc = run_bundle.start_doc

        yield "start", start_doc

        for stream_name in stream_names:

            data = np.random.random(data_shape)

            # Compose descriptor
            source = "NCEM"
            frame_data_keys = {
                "raw": {
                    "source": source,
                    "dtype": "array",
                    "shape": data.shape,
                    "dims": ("x", "y"),
                }
            }
            if not with_dims:
                del frame_data_keys['raw']['dims']

            frame_stream_bundle = run_bundle.compose_descriptor(
                data_keys=frame_data_keys, name=stream_name,
            )
            yield "descriptor", frame_stream_bundle.descriptor_doc

            yield "event", frame_stream_bundle.compose_event(
                data={"raw": data}, timestamps={"raw": time.time()}
            )

        yield "stop", run_bundle.compose_stop()

    return doc_gen(streams)


def test_multi_descriptors(multi_descriptor_doc_stream):
    docs = list(multi_descriptor_doc_stream)
    catalog = BlueskyInMemoryCatalog()
    start = docs[0][1]
    stop = docs[-1][1]

    def doc_gen():
        yield from docs

    catalog.upsert(start, stop, doc_gen, [], {})

    assert catalog[-1]["primary"].to_dask()["raw"].compute().shape == (
        stop["num_events"]["primary"],
        *data_shape,
    )
