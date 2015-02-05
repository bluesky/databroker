from __future__ import division
from metadataStore.api.collection import (insert_event,
                                          insert_event_descriptor)
from fileStore.api.analysis import (save_ndarray)
import numpy as np
from .common import example, noisy


# "Magic numbers" for this simulation
img_size = (150, 150)
period = 150
I_func_sin = lambda count: (1 + .5*np.sin(2 * count * np.pi / period))
center = 50
sigma = center / 4
I_func_gaus = lambda count: (1 + np.exp(-((count - center)/sigma) ** 2))


@example
def run(begin_run=None):
    # Make the data
    rs = np.random.RandomState(5)

    # set up the data keys entry
    data_keys1 = {'linear_motor': {'source': 'PV:ES:sam_x'},
                  'img': {'source': 'CCD', 'shape': (128, 128),
                          'external': 'FILESTORE:'},
                  'total_img_sum': {'source': 'CCD:sum'},
                  'img_x_max': {'source': 'CCD:xmax'},
                  'img_y_max': {'source': 'CCD:ymax'},
                  'img_sum_x': {'source': 'CCD:xsum', 'shape': (128,),
                                'external': 'FILESTORE:'},
                  'img_sum_y': {'source': 'CCD:ysum', 'shape': (128,),
                                'external': 'FILESTORE:'}
                  }
    data_keys2 = {'Tsam': {'source': 'PV:ES:Tsam'}}

    # save the first event descriptor
    e_desc1 = insert_event_descriptor(
        begin_run_event=begin_run, data_keys=data_keys1, time=0.)

    e_desc2 = insert_event_descriptor(
        begin_run_event=begin_run, data_keys=data_keys2, time=0.)

    # number of motor positions to fake
    num1 = center * 2
    # number of temperatures to record per motor position
    num2 = 10

    events = []
    for idx1, i in enumerate(range(num1)):
        img = rs.randint(0, 255, (128, 128))
        img_sum_x = img.sum(axis=0)
        img_sum_y = img.sum(axis=1)
        img_x_max = img_sum_x.argmax()
        img_y_max = img_sum_y.argmax()

        eid_img = save_ndarray(img)
        eid_x = save_ndarray(img_sum_x)
        eid_y = save_ndarray(img_sum_y)

        data1 = {'linear_motor': {'value': i, 'timestamp': noisy(i)},
                 'total_img_sum': {'value': img.sum(), 'timestamp': noisy(i)},
                 'img': {'value': eid_img, 'timestamp': noisy(i)},
                 'img_sum_x': {'value': eid_x, 'timestamp': noisy(i)},
                 'img_sum_y': {'value': eid_y, 'timestamp': noisy(i)},
                 'img_x_max': {'value': img_x_max, 'timestamp': noisy(i)},
                 'img_y_max': {'value': img_y_max, 'timestamp': noisy(i)},
                 }

        event = insert_event(event_descriptor=e_desc1, seq_no=idx1,
                             time=noisy(i), data=data1)
        events.append(event)
        for idx2, i2 in enumerate(range(num2)):
            time = noisy(i)
            data2 = {'Tsam': {'value': idx1 + np.random.randn()/100,
                              'timestamp': time}}
            event = insert_event(event_descriptor=e_desc2, seq_no=idx2+idx1,
                                 time=time, data=data2)
            events.append(event)

    return events
