import event_model
import os
import tempfile
import xarray
from ... import core
from ...core import documents_to_xarray


def no_event_pages(descriptor_uid):
    yield from ()


def event_page_gen(page_size, num_pages):
    """
    Generator event_pages for testing.
    """
    data_keys = ['x', 'y', 'z']
    array_keys = ['seq_num', 'time', 'uid']
    for i in range(num_pages):
        yield {'descriptor': 'DESCRIPTOR',
               **{key: list(range(i*page_size, (i+1)*page_size)) for key in array_keys},
               'data': {key: list(range(page_size)) for key in data_keys},
               'timestamps': {key: list(range(page_size)) for key in data_keys},
               'filled': {key: list(range(page_size)) for key in data_keys}}


def test_no_descriptors():
    run_bundle = event_model.compose_run()
    start_doc = run_bundle.start_doc
    stop_doc = run_bundle.compose_stop()
    documents_to_xarray(
        start_doc=start_doc,
        stop_doc=stop_doc,
        descriptor_docs=[],
        get_event_pages=no_event_pages,
        filler=event_model.Filler({}, inplace=True),
        get_resource=None,
        lookup_resource_for_datum=None,
        get_datum_pages=None)


def test_no_events():
    run_bundle = event_model.compose_run()
    start_doc = run_bundle.start_doc
    desc_bundle = run_bundle.compose_descriptor(
        data_keys={'x': {'source': '...', 'shape': [], 'dtype': 'number'}},
        name='primary')
    descriptor_doc = desc_bundle.descriptor_doc
    stop_doc = run_bundle.compose_stop()
    documents_to_xarray(
        start_doc=start_doc,
        stop_doc=stop_doc,
        descriptor_docs=[descriptor_doc],
        get_event_pages=no_event_pages,
        filler=event_model.Filler({}, inplace=True),
        get_resource=None,
        lookup_resource_for_datum=None,
        get_datum_pages=None)


def test_xarray_helpers():
    event_pages = list(event_page_gen(10, 5))
    dataarray_pages = [core.event_page_to_dataarray_page(page) for page in event_pages]
    dataarray_page = core.concat_dataarray_pages(dataarray_pages)
    dataset_page = core.dataarray_page_to_dataset_page(dataarray_page)
    assert isinstance(dataset_page['data'], xarray.Dataset)
    assert isinstance(dataset_page['timestamps'], xarray.Dataset)
    assert isinstance(dataset_page['filled'], xarray.Dataset)


def test_interlace_event_page_chunks():
    page_gens = [event_page_gen(10, 5) for i in range(3)]
    interlaced = core.interlace_event_page_chunks(*page_gens, chunk_size=3)

    t0 = None
    for chunk in interlaced:
        t1 = chunk['time'][0]
        if t0:
            assert t1 > t0
        t0 = t1


def test_tail():
    with tempfile.TemporaryDirectory() as tempdir:
        with open(os.path.join(tempdir, 'lastlines_test.txt'), 'w') as f:
            for i in range(1000):
                f.write(f'{i}\n')
            filename = f.name
        assert list(core.tail(filename, n=2)) == ['998', '999']
