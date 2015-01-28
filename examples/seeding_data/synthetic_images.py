__author__ = 'arkilic'

from metadataStore.api.collection import (save_header, save_beamline_config,
                                          save_event, save_event_descriptor)
from metadataStore.api.analysis import find_last
import random
import time
import string
import numpy as np
from skxray import core
from frame_source import FrameSourcerBrownian
import uuid

# used below
img_size = (150, 150)
period = 150
I_func_sin = lambda count: (1 + .5*np.sin(2 * count * np.pi / period))
center = 500
sigma = center / 4
I_func_gaus = lambda count: (1 + np.exp(-((count - center)/sigma) ** 2))


def scale_fluc(scale, count):
    if not count % 50:
        return scale - .5
    if not count % 25:
        return scale + .5
    return None

frame_source = FrameSourcerBrownian(img_size, delay=1, step_scale=.5,
                                    I_fluc_function=I_func_gaus,
                                    step_fluc_function=scale_fluc,
                                    max_count=center*2
                                    )

b_config = save_beamline_config(config_params={'my_beamline': 'my_value'})

# set up the data keys entry
data_keys1 = {'linear_motor': {'source': 'ES:sam_x'},
              'img': {'source': 'CCD', 'external': 'FILESTORE'},
              'total_img_sum': {'source': 'CCD:sum'},
              'img_x_max': {'source': 'CCD:xmax'},
              'img_y_max': {'source': 'CCD:ymax'},
              'img_sum_x': {'source': 'CCD:xsum'},
              'img_sum_y': {'source': 'CCD:ysum'},
}
data_keys2 = {'Tsam': {'source': 'ES:Tsam'}}
# save the first event descriptor
e_desc1 = save_event_descriptor(event_type_id=1, data_keys=data_keys1,
                               descriptor_name=uuid.uuid4())

e_desc2 = save_event_descriptor(event_type_id=2, data_keys=data_keys2,
                               descriptor_name=uuid.uuid4())

try:
    last_hdr = find_last()
    scan_id = last_hdr.scan_id+1
except IndexError:
    scan_id = 1

h = save_header(unique_id=uuid.uuid4(), scan_id=scan_id,
                create_time=time.time(), beamline_config=b_config,
                event_descriptors=[e_desc1, e_desc2])

# number of motor positions to fake
num1 = center * 2
# number of temperatures to record per motor position
num2 = 10

sleep_time = 1 # in seconds

for idx1, i in enumerate(range(num1)):
    img = frame_source.gen_next_frame()
    img_sum_x = img.sum(axis=0)
    img_sum_y = img.sum(axis=1)
    img_x_max = img_sum_x.argmax()
    img_y_max = img_sum_y.argmax()
    # still need some magic way to save data into the file store, and I really
    # have no idea how the file store works
    data1 = {'linear_motor': i,
            'total_img_sum': img.sum(),
            'img_sum_x': img_sum_x,
            'img_sum_y': img_sum_y,
            'img_x_max': img_x_max,
            'img_y_max': img_y_max,
            }
    save_event(header=h, event_descriptor=e_desc1, seq_no=idx1,
               beamline_id='csx', timestamp=time.time(), data=data1)
    for idx2, i2 in enumerate(range(num2)):
        data2 = {'Tsam': idx1 + np.random.randn()/100}
        save_event(header=h, event_descriptor=e_desc2, seq_no=idx2+idx1,
                   beamline_id='csx', timestamp=time.time(), data=data2)
    time.sleep(sleep_time)
