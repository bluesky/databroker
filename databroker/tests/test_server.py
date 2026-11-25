import asyncio
import uuid

from tiled.client import Context
from tiled.server.app import build_app
from tiled.utils import safe_json_dump

from ..mongo_normalized import MongoAdapter


class RecordingSerializer:
    "Wrap a Serializer. Record whether it was called on the event loop thread."

    def __init__(self, serializer):
        self._serializer = serializer
        self.called_on_event_loop = []

    def __call__(self, name, doc):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self.called_on_event_loop.append(False)
        else:
            self.called_on_event_loop.append(True)
        return self._serializer(name, doc)


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
