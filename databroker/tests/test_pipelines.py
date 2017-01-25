import time
import tempfile
import os
import uuid
from functools import partial
import numpy as np
from databroker.broker import store_dec, event_map


class NpyWriter:
    """
    Each call to the ``write`` method saves a file and creates a new filestore
    resource and datum record.
    """

    SPEC = 'npy'

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
        resource = self._fs.insert_resource(self.SPEC, "{}.npy".format(fp),
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
        if name == 'start':
            assert doc['parents'] == [input_hdr['start']['uid']]
            output_uid = doc['uid']
        if name == 'event':
            np.testing.assert_array_equal(doc['data']['image'], 2 * IMG)
    output_hdr = db[output_uid]
    for ev1, ev2 in zip(db.get_events(input_hdr, fill=True),
                        db.get_events(output_hdr, fill=True)):
        assert ev1['data']['image'] == ev2['data']['image']
