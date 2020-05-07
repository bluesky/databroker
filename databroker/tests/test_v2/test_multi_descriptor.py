from pytest import fixture
import pytest
import event_model
import numpy as np
import time
from databroker.in_memory import BlueskyInMemoryCatalog

data_shape = (1000, 1000)


@fixture
def multi_descriptor_doc_stream(request):
    streams = request.param
    def doc_gen(stream_names):

        # Compose run start
        run_bundle = event_model.compose_run()  # type: event_model.ComposeRunBundle
        start_doc = run_bundle.start_doc

        yield 'start', start_doc

        for stream_name in stream_names:

            data = np.random.random(data_shape)

            # Compose descriptor
            source = 'NCEM'
            frame_data_keys = {'raw': {'source': source,
                                       'dtype': 'number',
                                       'shape': data.shape}}
            frame_stream_bundle = run_bundle.compose_descriptor(data_keys=frame_data_keys,
                                                                name=stream_name,
                                                                )
            yield 'descriptor', frame_stream_bundle.descriptor_doc

            yield 'event', frame_stream_bundle.compose_event(data={'raw': data},
                                                             timestamps={'raw': time.time()})

        yield 'stop', run_bundle.compose_stop()
    return doc_gen(streams)


def _test_ingest_to_xarray(stream):
    docs = list(stream)
    catalog = BlueskyInMemoryCatalog()
    start = docs[0][1]
    stop = docs[-1][1]

    def doc_gen():
        yield from docs

    catalog.upsert(start, stop, doc_gen, [], {})

    assert catalog[-1]['primary'].to_dask()['raw'].compute().shape == (1, *data_shape)


@pytest.mark.parametrize("multi_descriptor_doc_stream", (["primary", "baseline"],), indirect=True)
def test_multi_descriptors_unique(multi_descriptor_doc_stream):
    _test_ingest_to_xarray(multi_descriptor_doc_stream)


@pytest.mark.parametrize("multi_descriptor_doc_stream", (["primary", "primary"],), indirect=True)
def test_multi_descriptors_same(multi_descriptor_doc_stream):
    _test_ingest_to_xarray(multi_descriptor_doc_stream)

