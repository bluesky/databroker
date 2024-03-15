import collections
import time

from event_model import unpack_datum_page, unpack_event_page


def replay(gen, callback, burst=False, delay=0):
    """
    Emit documents to a callback with realistic time spacing.

    Parameters
    ----------
    gen: iterable
        Expected to yield (name, doc) pairs
    callback: callable
        Expected signature: callback(name, doc)
    """
    cache = collections.deque()
    name, doc = next(gen)
    if name != "start":
        raise ValueError("Expected gen to start with a RunStart document")
    # Compute time difference between now and the time that this run started.
    offset = time.time() - doc["time"]
    callback(name, doc)
    for name, doc in gen:
        if name == "event_page":
            # Expand this EventPage into Events.
            for event in unpack_event_page(doc):
                _process_document("event", event, cache, offset, callback, burst, delay)
        elif name == "datum_page":
            # Expand this DatumgPage into Events.
            for datum in unpack_datum_page(doc):
                _process_document("datum", datum, cache, offset, callback, burst, delay)
        else:
            _process_document(name, doc, cache, offset, callback, burst, delay)


_DOCUMENTS_WITHOUT_A_TIME = {"datum", "datum_page", "resource"}


def _process_document(name, doc, cache, offset, callback, burst, delay):
    if name in _DOCUMENTS_WITHOUT_A_TIME:
        # The bluesky RunEngine emits these documents immediately
        # before emitting an Event, which does have a time. Lacking
        # more specific timing info, we'll cache these and then emit
        # them in a burst before the next document with an associated time.
        cache.append((name, doc))
    else:
        if not burst:
            delay = max(0, offset - (time.time() - doc["time"]))
            time.sleep(delay)
        while cache:
            # Emit any cached documents without a time in a burst.
            time.sleep(delay)
            callback(*cache.popleft())
        # Emit this document.
        time.sleep(delay)
        callback(name, doc)
