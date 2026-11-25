import asyncio
import uuid

from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.utils import safe_json_dump

from ..mongo_normalized import MongoAdapter


class RecordingSerializer:
    "Wrap a Serializer. Record whether it was called on the event loop thread."

    def __init__(self, serializer):
        self._serializer = serializer
        self.called_on_event_loop = []
        self.update_called_on_event_loop = []

    @staticmethod
    def _record(record):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            record.append(False)
        else:
            record.append(True)

    def __call__(self, name, doc):
        self._record(self.called_on_event_loop)
        return self._serializer(name, doc)

    def update(self, name, doc):
        self._record(self.update_called_on_event_loop)
        return self._serializer.update(name, doc)


def test_post_document_inserts_off_the_event_loop():
    """
    Inserting a document blocks while it writes to MongoDB. The server must
    do that in a worker thread, or one slow insert stalls every other request
    handled by the same process.
    """
    adapter = MongoAdapter.from_mongomock()
    recorder = RecordingSerializer(adapter.get_serializer())
    adapter._serializer = recorder
    uid = str(uuid.uuid4())
    with Context.from_app(build_app(adapter)) as context:

        def post(name, doc):
            return context.http_client.post(
                "/api/v1/documents/", content=safe_json_dump({"name": name, "doc": doc})
            )

        response = post("start", {"uid": uid, "time": 0.0})
        assert response.status_code == 200
        # A document that is not valid is still refused, and not inserted.
        response = post("start", {"time": 0.0})
        assert response.status_code == 400
    assert adapter[uid].metadata()["start"]["uid"] == uid  # It was inserted...
    assert recorder.called_on_event_loop == [False]  # ...once, in a thread.


def test_update_metadata_off_the_event_loop():
    """
    Updating the metadata of a run, or of a stream, reads from and writes to
    MongoDB. That must not happen on the event loop either.
    """
    adapter = MongoAdapter.from_mongomock()
    serializer = adapter.get_serializer()
    recorder = RecordingSerializer(serializer)
    adapter._serializer = recorder
    uid = str(uuid.uuid4())
    serializer("start", {"uid": uid, "time": 0.0})
    serializer(
        "descriptor",
        {
            "uid": str(uuid.uuid4()),
            "run_start": uid,
            "time": 1.0,
            "name": "primary",
            "data_keys": {},
            "object_keys": {},
            "configuration": {},
        },
    )
    serializer("stop", {"uid": str(uuid.uuid4()), "run_start": uid, "time": 2.0, "exit_status": "success"})
    record = recorder.update_called_on_event_loop
    with Context.from_app(build_app(adapter)) as context:
        client = from_context(context)

        # a run: its start and stop documents
        client[uid].update_metadata({"start": {"sample": "new"}, "stop": {"reason": "new"}})
        assert client[uid].metadata["start"]["sample"] == "new"
        assert client[uid].metadata["stop"]["reason"] == "new"
        assert record == [False, False]  # start and stop, each in a thread

        # a stream: its descriptors
        record.clear()
        stream = client[uid]["primary"]
        metadata = stream.metadata_copy()[0]
        metadata["descriptors"][0]["configuration"] = {"det": {}}
        stream.update_metadata(metadata=metadata)
        assert client[uid]["primary"].metadata["descriptors"][0]["configuration"] == {"det": {}}
        assert record == [False]
