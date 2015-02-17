from __future__ import division
import uuid
from metadatastore.api import insert_event, insert_event_descriptor
from filestore.api.analysis import save_ndarray
from ...broker.simple_broker import fill_event
import numpy as np
from .common import example, noisy


@example
def run(run_start=None):
    # Make the data
    rs = np.random.RandomState(5)

    # set up the data keys entry
    data_keys1 = {'linear_motor': dict(source='PV:ES:sam_x', dtype='number'),
                  'img': dict(source='CCD', shape=(5, 5), dtype='array',
                              external='FILESTORE:'),
                  'total_img_sum': dict(source='CCD:sum', dtype='number'),
                  'img_x_max': dict(source='CCD:xmax', dtype='number'),
                  'img_y_max': dict(source='CCD:ymax', dtype='number'),
                  'img_sum_x': dict(source='CCD:xsum', dtype='array',
                                    shape=(5,), external='FILESTORE:'),
                  'img_sum_y': dict(source='CCD:ysum', dtype='array',
                                    shape=(5,), external='FILESTORE:')
                  }
    data_keys2 = {'Tsam': dict(source='PV:ES:Tsam', dtype='number')}

    # save the first event descriptor
    e_desc1 = insert_event_descriptor(
        run_start=run_start, data_keys=data_keys1, time=0.,
        uid=str(uuid.uuid4()))

    e_desc2 = insert_event_descriptor(
        run_start=run_start, data_keys=data_keys2, time=0.,
        uid=str(uuid.uuid4()))

    # number of motor positions to fake
    num1 = 20
    # number of temperatures to record per motor position
    num2 = 10

    events = []
    for idx1, i in enumerate(range(num1)):
        img = np.ones((5, 5))
        img_sum = float(img.sum())
        img_sum_x = img.sum(axis=0)
        img_sum_y = img.sum(axis=1)
        img_x_max = float(img_sum_x.argmax())
        img_y_max = float(img_sum_y.argmax())

        fsid_img = save_ndarray(img)
        fsid_x = save_ndarray(img_sum_x)
        fsid_y = save_ndarray(img_sum_y)

        # Put in actual ndarray data, as broker would do.
        data1 = {'linear_motor': (i, noisy(i)),
                 'total_img_sum': (img_sum, noisy(i)),
                 'img': (fsid_img, noisy(i)),
                 'img_sum_x': (fsid_x, noisy(i)),
                 'img_sum_y': (fsid_y, noisy(i)),
                 'img_x_max': (img_x_max, noisy(i)),
                 'img_y_max': (img_y_max, noisy(i))
                 }

        event = insert_event(event_descriptor=e_desc1, seq_num=idx1,
                             time=noisy(i), data=data1, uid=str(uuid.uuid4()))
        fill_event(event)
        events.append(event)
        for idx2, i2 in enumerate(range(num2)):
            time = noisy(i/num2)
            data2 = {'Tsam': (idx1 + np.random.randn()/100, time)}
            event = insert_event(event_descriptor=e_desc2, seq_num=idx2+idx1,
                                 time=time, data=data2, uid=str(uuid.uuid4()))
            events.append(event)

    return events
