import event_model
from intake_bluesky.core import BlueskyEventStream, DaskFiller

def no_event_pages(descriptor_uid):
    yield from ()

def test_no_descriptors():
    run_bundle = event_model.compose_run()
    start_doc = run_bundle.start_doc
    stop_doc = run_bundle.compose_stop()

    entry_metadata = {'start': start_doc,
                      'stop': stop_doc}

    bes = BlueskyEventStream(
        get_run_start=lambda: start_doc,
        stream_name=True,
        get_run_stop=lambda: stop_doc,
        get_event_descriptors= lambda: [],
        get_event_pages=no_event_pages,
        get_event_count=lambda: 0,
        get_resource=None,
        lookup_resource_for_datum=None,
        get_datum_pages=None,
        filler=DaskFiller(),
        metadata=entry_metadata)

    bes._open_dataset()


def test_no_events():
    run_bundle = event_model.compose_run()
    start_doc = run_bundle.start_doc
    desc_bundle = run_bundle.compose_descriptor(
        data_keys={'x': {'source': '...', 'shape': [], 'dtype': 'number'}},
        name='primary')
    descriptor_doc = desc_bundle.descriptor_doc
    stop_doc = run_bundle.compose_stop()

    entry_metadata = {'start': start_doc,
                      'stop': stop_doc}

    bes = BlueskyEventStream(
        get_run_start=lambda: start_doc,
        stream_name=True,
        get_run_stop=lambda: stop_doc,
        get_event_descriptors= lambda: [descriptor_doc],
        get_event_pages=no_event_pages,
        get_event_count=lambda: 0,
        get_resource=None,
        lookup_resource_for_datum=None,
        get_datum_pages=None,
        filler=DaskFiller(),
        metadata=entry_metadata)

    bes._open_dataset()
