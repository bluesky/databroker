import time
import tempfile
import os
import uuid
from functools import partial
import numpy as np
from databroker.broker import store_dec, event_map
from numpy.testing import assert_array_equal
from databroker.tests.utils import py3
from pprint import pprint


class NpyWriter:
    """
    Each call to the ``write`` method saves a file and creates a new filestore
    resource and datum record.
    """

    SPEC = 'npy'
    EXT = 'npy'

    def __init__(self, fs, root):
        self._root = root
        self._closed = False
        self._fs = fs
        # Open and stash a file handle (e.g., h5py.File) if applicable.

    def write(self, data):
        """
        Save data to file, generate and insert new resource and datum.
        """
        if self._closed:
            raise RuntimeError('This writer has been closed.')
        fp = os.path.join(self._root, '{}.npy'.format(str(uuid.uuid4())))
        np.save(fp, data)
        resource = self._fs.insert_resource(self.SPEC, fp + self.EXT,
                                            resource_kwargs={})
        datum_id = str(uuid.uuid4())
        self._fs.insert_datum(resource=resource, datum_id=datum_id,
                              datum_kwargs={})
        return datum_id

    def close(self):
        self._closed = True

    def __enter__(self): return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


@py3
def test_streaming(db, RE):
    ### GENERATE 'RAW' DATA ###
    from bluesky.examples import (Reader, ReaderWithFileStore,
                                  ReaderWithFSHandler)
    import bluesky.plans as bp
    IMG = np.ones((13, 11))
    dirname = tempfile.TemporaryDirectory().name  # where image_det will save
    os.makedirs(dirname)
    image_det = ReaderWithFileStore('image_det',
                                    {'image': lambda: IMG},
                                    fs=db.fs,
                                    save_path=dirname)
    db.fs.register_handler('RWFS_NPY', ReaderWithFSHandler)
    RE.subscribe('all', db.mds.insert)
    input_uid, = RE(bp.count([image_det]))

    dirname = tempfile.TemporaryDirectory().name
    os.makedirs(dirname)

    ### DEFINE AN ANALYSIS PIPELINE ###
    @store_dec(db, {'image': partial(NpyWriter, root=dirname)})
    @event_map('primary', {'image': {}}, {})
    def multiply_by_two(arr):
        return arr * 2

    ### APPLY THE PIPELINE ###
    # (This put analyzed data in the same db as the raw data, which is not
    # encouraged but is simpler to do for this test.)
    input_hdr = db[input_uid]
    input_stream = db.restream(input_hdr, fill=True)
    output_stream = multiply_by_two(input_stream)
    for name, doc in output_stream:
        # pprint(name)
        # pprint(doc)
        if name == 'start':
            assert doc['parents'] == [input_hdr['start']['uid']]
            output_uid = doc['uid']
        if name == 'stop':
            assert doc['exit_status'] == 'success'
        if name == 'event':
            assert doc['data']['image'] == 2 * IMG
            assert 'filled' in doc.keys()
    output_hdr = db[output_uid]
    for ev1, ev2 in zip(db.get_events(input_hdr, fill=True),
                        db.get_events(output_hdr, fill=True)):
        pprint(ev1)
        pprint(ev2)
        AAA
        assert ev1['data']['image'] == ev2['data']['image']
        assert_array_equal(ev1['data']['image'], ev2['data']['image'] * 2)


@py3
def test_almost_live_streaming(db, RE, tmp_dir):
    ### GENERATE 'RAW' DATA ###
    from bluesky.examples import (Reader, ReaderWithFileStore,
                                  ReaderWithFSHandler)
    import bluesky.plans as bp
    from multiprocessing import Queue
    Q = Queue()

    def subs_Q(*args):
        Q.put(args)

    GQ = iter(Q.get_nowait, None)
    RE.subscribe('all', subs_Q)

    IMG = np.ones((13, 11))
    dirname = tmp_dir  # where image_det will save

    image_det = ReaderWithFileStore('image_det',
                                    {'image': lambda: IMG},
                                    fs=db.fs,
                                    save_path=dirname)
    db.fs.register_handler('RWFS_NPY', ReaderWithFSHandler)
    RE.subscribe('all', db.mds.insert)

    ### DEFINE AN ANALYSIS PIPELINE ###
    @store_dec(db, {'image': partial(NpyWriter, root=dirname)})
    @event_map('primary', {'image': {}}, {})
    def multiply_by_two(arr):
        return arr * 2

    input_uid, = RE(bp.count([image_det]))

    ### APPLY THE PIPELINE ###
    # (This put analyzed data in the same db as the raw data, which is not
    # encouraged but is simpler to do for this test.)
    input_hdr = db[input_uid]
    output_stream = multiply_by_two(GQ)
    for name, doc in output_stream:
        if name == 'start':
            assert doc['parents'] == [input_hdr['start']['uid']]
            output_uid = doc['uid']
        if name == 'event':
            assert doc['data']['image'] == 2 * IMG
        if Q.empty():
            break
    output_hdr = db[output_uid]
    for ev1, ev2 in zip(db.get_events(input_hdr, fill=True),
                        db.get_events(output_hdr, fill=True)):
        assert ev1['data']['image'] == ev2['data']['image']
        assert_array_equal(ev1['data']['image'], ev2['data']['image'] * 2)


@py3
def test_live_streaming(db, RE, tmp_dir):
    from portable_mds.sqlite.mds import MDS
    import pytest
    if isinstance(db.mds, MDS):
        pytest.skip('Some reason this fails with sqlite idk why')
    ### GENERATE 'RAW' DATA ###
    from bluesky.examples import (Reader, ReaderWithFileStore,
                                  ReaderWithFSHandler)
    import bluesky.plans as bp
    from multiprocessing import Queue, Process
    import queue
    from time import sleep
    Q = Queue()
    rq = Queue()

    def subs_Q(*args):
        Q.put(args)

    GQ = iter(Q.get, None)
    RGQ = iter(rq.get, None)
    RE.subscribe('all', subs_Q)

    IMG = np.ones((13, 11))
    dirname = tmp_dir  # where image_det will save

    image_det = ReaderWithFileStore('image_det',
                                    {'image': lambda: IMG},
                                    fs=db.fs,
                                    save_path=dirname)
    db.fs.register_handler('RWFS_NPY', ReaderWithFSHandler)
    RE.subscribe('all', db.mds.insert)

    ### DEFINE AN ANALYSIS PIPELINE ###
    @store_dec(db, {'image': partial(NpyWriter, root=dirname)})
    @event_map('primary', {'image': {}}, {})
    def multiply_by_two(arr):
        return arr * 2

    def process_data(GQ, pipeline, **kwargs):
        while True:
            # make the generator
            pipeline_gen = pipeline(GQ, **kwargs)
            while True:
                # pull the documents
                name, doc = next(pipeline_gen)
                rq.put((name, doc))
                if name == 'stop':
                    # Take it from the top!
                    break

    p = Process(target=process_data, args=(GQ, multiply_by_two))
    p.start()
    input_uid, = RE(bp.count([image_det]))

    ### APPLY THE PIPELINE ###
    # (This put analyzed data in the same db as the raw data, which is not
    # encouraged but is simpler to do for this test.)
    input_hdr = db[input_uid]
    for name, doc in RGQ:
        if name == 'start':
            assert doc['parents'] == [input_hdr['start']['uid']]
            output_uid = doc['uid']
        if name == 'event':
            assert doc['data']['image'] == 2 * IMG
        if rq.empty():
            p.terminate()
            break

    output_hdr = db[output_uid]
    for ev1, ev2 in zip(db.get_events(input_hdr, fill=True),
                        db.get_events(output_hdr, fill=True)):
        assert ev1['data']['image'] == ev2['data']['image']
        assert_array_equal(ev1['data']['image'], ev2['data']['image'] * 2)
