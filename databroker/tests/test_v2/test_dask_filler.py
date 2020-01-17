from event_model import Filler
import event_model
from bluesky.plans import count
import numpy
from ophyd.sim import NumpySeqHandler


def test_fill_event(RE, hw):

    docs = []

    def callback(name, doc):
        docs.append((name, doc))

    RE(count([hw.img]), callback)
    docs
    dask_filler = Filler({'NPY_SEQ': NumpySeqHandler}, coerce='delayed',
                         inplace=False)
    filled_docs = []
    for name, doc in docs:
        filled_docs.append(dask_filler(name, doc))
    filled_docs
    _, dask_filled_event = filled_docs[-2]
    arr = dask_filled_event['data']['img'].compute()
    assert arr.shape == (10, 10)
    assert isinstance(arr, numpy.ndarray)


def test_fill_event_page(RE, hw):
    docs = []

    def callback(name, doc):
        docs.append((name, doc))

    RE(count([hw.img]), callback)
    docs
    dask_filler = Filler({'NPY_SEQ': NumpySeqHandler}, coerce='delayed',
                         inplace=False)
    filled_docs = []
    _, event = docs[-2]
    event_page = event_model.pack_event_page(event)
    docs[-2] = ('event_page', event_page)
    dask_filler = Filler({'NPY_SEQ': NumpySeqHandler}, coerce='delayed',
                         inplace=False)
    filled_docs = []
    for name, doc in docs:
        filled_docs.append(dask_filler(name, doc))
    _, dask_filled_event_page = filled_docs[-2]
    arr = dask_filled_event_page['data']['img'][0].compute()
    assert arr.shape == (10, 10)
    assert isinstance(arr, numpy.ndarray)
