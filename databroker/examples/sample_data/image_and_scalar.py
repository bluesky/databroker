from __future__ import division
from metadataStore.api.collection import (insert_event,
                                          insert_event_descriptor)
from fileStore.api.analysis import save_ndarray
from ...broker.simple_broker import fill_event
import numpy as np
from .common import example, noisy


@example
def run(begin_run=None):
    # Make the data
    rs = np.random.RandomState(5)

    # set up the data keys entry
    data_keys1 = {'linear_motor': {'source': 'PV:ES:sam_x'},
                  'img': {'source': 'CCD', 'shape': (5, 5),
                          'external': 'FILESTORE:'},
                  'total_img_sum': {'source': 'CCD:sum'},
                  'img_x_max': {'source': 'CCD:xmax'},
                  'img_y_max': {'source': 'CCD:ymax'},
                  'img_sum_x': {'source': 'CCD:xsum', 'shape': (5,),
                                'external': 'FILESTORE:'},
                  'img_sum_y': {'source': 'CCD:ysum', 'shape': (5,),
                                'external': 'FILESTORE:'}
                  }
    data_keys2 = {'Tsam': {'source': 'PV:ES:Tsam'}}

    # save the first event descriptor
    e_desc1 = insert_event_descriptor(
        begin_run_event=begin_run, data_keys=data_keys1, time=0.)

    e_desc2 = insert_event_descriptor(
        begin_run_event=begin_run, data_keys=data_keys2, time=0.)

    # number of motor positions to fake
    num1 = 20
    # number of temperatures to record per motor position
    num2 = 10

    events = []
    for idx1, i in enumerate(range(num1)):
        img = np.zeros((5, 5))
        img_sum = float(img.sum())
        img_sum_x = img.sum(axis=0)
        img_sum_y = img.sum(axis=1)
        img_x_max = float(img_sum_x.argmax())
        img_y_max = float(img_sum_y.argmax())

        fsid_img = save_ndarray(img)
        fsid_x = save_ndarray(img_sum_x)
        fsid_y = save_ndarray(img_sum_y)

        # Put in actual ndarray data, as broker would do.
        data1 = {'linear_motor': {'value': i, 'timestamp': noisy(i)},
                 'total_img_sum': {'value': img_sum, 'timestamp': noisy(i)},
                 'img': {'value': fsid_img, 'timestamp': noisy(i)},
                 'img_sum_x': {'value': fsid_x, 'timestamp': noisy(i)},
                 'img_sum_y': {'value': fsid_y, 'timestamp': noisy(i)},
                 'img_x_max': {'value': img_x_max, 'timestamp': noisy(i)},
                 'img_y_max': {'value': img_y_max, 'timestamp': noisy(i)},
                 }

        event = insert_event(event_descriptor=e_desc1, seq_no=idx1,
                             time=noisy(i), data=data1)
        fill_event(event)
        events.append(event)
        for idx2, i2 in enumerate(range(num2)):
            time = noisy(i/num2)
            data2 = {'Tsam': {'value': idx1 + np.random.randn()/100,
                              'timestamp': time}}
            event = insert_event(event_descriptor=e_desc2, seq_no=idx2+idx1,
                                 time=time, data=data2)
            events.append(event)

    return events
