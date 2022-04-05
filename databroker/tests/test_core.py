import event_model
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

