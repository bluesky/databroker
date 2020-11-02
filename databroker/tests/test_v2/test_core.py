import event_model
import os
import tempfile
import xarray
from ... import core
from bluesky_live.conversion import documents_to_xarray


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
    dataarray_pages = [core._event_page_to_dataarray_page(page) for page in event_pages]
    dataarray_page = core._concat_dataarray_pages(dataarray_pages)
    dataset_page = core._dataarray_page_to_dataset_page(dataarray_page)
    assert isinstance(dataset_page['data'], xarray.Dataset)
    assert isinstance(dataset_page['timestamps'], xarray.Dataset)
    assert isinstance(dataset_page['filled'], xarray.Dataset)


def test_interlace_event_page_chunks():
    page_gens = [event_page_gen(10, 5) for i in range(3)]
    interlaced = core._interlace_event_page_chunks(*page_gens, chunk_size=3)

    t0 = None
    total_events = 0
    for j, chunk in enumerate(interlaced):
        total_events += len(chunk['seq_num'])
        t1 = chunk['time'][0]
        if t0:
            assert t1 >= t0
        t0 = t1
    expected_page_count = (50 // 3 + 1) * 3
    assert j + 1 == expected_page_count
    assert 10*5*3 == total_events


def test_single_run_cache(hw, detector, RE):
    import bluesky
    from databroker.core import SingleRunCache

    def plan(num_points=11):
        src = SingleRunCache()

        @bluesky.preprocessors.subs_decorator(src.callback)
        def inner_plan():
            yield from bluesky.plans.rel_scan([detector], hw.motor, -1, 1,
                                              num_points)
            run = src.retrieve()
            table = run.primary.read()['motor'].to_dataframe()
            assert len(table) == num_points

        yield from inner_plan()

    RE(plan())
