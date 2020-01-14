# This is a special test because we corrupt the generated data.
# That is why it does not reuse the standard fixures.

import tempfile
from suitcase.jsonl import Serializer
from bluesky import RunEngine
from bluesky.plans import count
from ophyd.sim import det
from databroker._drivers.jsonl import BlueskyJSONLCatalog


def test_no_stop_document(RE, tmpdir):
    """
    When a Run has no RunStop document, whether because it does not exist yet
    or because the Run was interrupted in a critical way and never completed,
    we expect the field for 'stop' to contain None.
    """
    directory = str(tmpdir)

    serializer = Serializer(directory)

    def insert_all_except_stop(name, doc):
        if name != 'stop':
            serializer(name, doc)

    RE(count([det]), insert_all_except_stop)
    serializer.close()
    catalog = BlueskyJSONLCatalog(f'{directory}/*.jsonl')
    assert catalog[-1].metadata['start'] is not None
    assert catalog[-1].metadata['stop'] is None
