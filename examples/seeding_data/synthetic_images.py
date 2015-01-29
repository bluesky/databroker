__author__ = 'arkilic'

from metadataStore.api.collection import (insert_begin_run, insert_event,
                                          insert_beamline_config,
                                          insert_event_descriptor)
from metadataStore.api.analysis import find_last
import random
import time
import string
import numpy as np
from skxray import core
from frame_source import FrameSourcerBrownian
import uuid
from fileStore.api import analysis as fsa

# used below
img_size = (150, 150)
period = 150
I_func_sin = lambda count: (1 + .5*np.sin(2 * count * np.pi / period))
center = 50
sigma = center / 4
I_func_gaus = lambda count: (1 + np.exp(-((count - center)/sigma) ** 2))


def scale_fluc(scale, count):
    if not count % 50:
        return scale - .5
    if not count % 25:
        return scale + .5
    return None

frame_source = FrameSourcerBrownian(img_size, step_scale=.5,
                                    I_fluc_function=I_func_gaus,
                                    step_fluc_function=scale_fluc,
                                    )

b_config = insert_beamline_config(time=time.time(), config_params={'my_beamline': 'my_value'})
b_config=None
try:
    last_start_event = find_last()
    scan_id = last_start_event.scan_id+1
except IndexError:
    scan_id = 1

# insert the begin run event
bre = insert_begin_run(scan_id=scan_id, time=time.time(), beamline_id='csx',
                       beamline_config=b_config)

# set up the data keys entry
data_keys1 = {'linear_motor': {'source': 'PV:ES:sam_x'},
              'img': {'source': 'CCD', 'external': 'FILESTORE:'},
              'total_img_sum': {'source': 'CCD:sum'},
              'img_x_max': {'source': 'CCD:xmax'},
              'img_y_max': {'source': 'CCD:ymax'},
              'img_sum_x': {'source': 'CCD:xsum', 'external': 'FILESTORE:'},
              'img_sum_y': {'source': 'CCD:ysum', 'external': 'FILESTORE:'},
}
data_keys2 = {'Tsam': {'source': 'PV:ES:Tsam'}}

# save the first event descriptor
e_desc1 = insert_event_descriptor(begin_run_event=bre, data_keys=data_keys1,
                                  time=time.time())

e_desc2 = insert_event_descriptor(begin_run_event=bre, data_keys=data_keys2,
                                  time=time.time())

# number of motor positions to fake
num1 = center * 2
# number of temperatures to record per motor position
num2 = 10

sleep_time = 0 # in seconds

for idx1, i in enumerate(range(num1)):
    img = frame_source.gen_next_frame()
    img_sum_x = img.sum(axis=0)
    img_sum_y = img.sum(axis=1)
    img_x_max = img_sum_x.argmax()
    img_y_max = img_sum_y.argmax()
    eid_img = fsa.save_ndarray(img)
    eid_x = fsa.save_ndarray(img_sum_x)
    eid_y = fsa.save_ndarray(img_sum_y)
    # still need some magic way to save data into the file store, and I really
    # have no idea how the file store works
    data1 = {'linear_motor': {'value': i, 'timestamp': time.time()},
            'total_img_sum': {'value': img.sum(), 'timestamp': time.time()},
            'img': {'value': eid_img, 'timestamp': time.time()},
            'img_sum_x': {'value': eid_x, 'timestamp': time.time()},
            'img_sum_y': {'value': eid_y, 'timestamp': time.time()},
            'img_x_max': {'value': img_x_max, 'timestamp': time.time()},
            'img_y_max': {'value': img_y_max, 'timestamp': time.time()},
            }
    insert_event(event_descriptor=e_desc1, seq_no=idx1, time=time.time(),
                 data=data1)
    for idx2, i2 in enumerate(range(num2)):
        data2 = {'Tsam': {'value': idx1 + np.random.randn()/100,
                          'timestamp': time.time()}}
        insert_event(event_descriptor=e_desc2, seq_no=idx2+idx1,
                     time=time.time(), data=data2)
    # time.sleep(sleep_time)
