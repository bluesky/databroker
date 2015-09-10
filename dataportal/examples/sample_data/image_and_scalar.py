from __future__ import division
import uuid
import time as ttime
import numpy as np
from metadatastore.api import (insert_event, insert_descriptor,
                               find_events, insert_runstart, insert_runstop,
                               find_last)
from filestore.file_writers import save_ndarray
from dataportal.examples.sample_data import frame_generators
from dataportal.examples.sample_data import common
from dataportal.examples.sample_data.common_nonscalar import nonscalar_example
import argparse


# number of motor positions to fake
num1 = 20
# number of temperatures to record per motor position
num2 = 10

# These are imported from MDS and cannot be imported as things.
noisy = common.noisy
example = common.example

# This section sets up what the simulated images will look like.

img_size = (500, 500)
period = 150


def I_func_sin(count):
    return (1 + .5*np.sin(2 * count * np.pi / period))
center = 25
sigma = center / 4


def I_func_gaus(count):
    return (1 + np.exp(-((count - center)/sigma) ** 2))


def scale_fluc(scale, count):
    if not count % 50:
        return scale - .5
    if not count % 25:
        return scale + .5
    return None


# And this section is the actual example.

@nonscalar_example
@example
def run(runstart_uid=None, sleep=0):
    frame_generator = frame_generators.brownian(img_size, step_scale=.5,
                                                I_fluc_function=I_func_gaus,
                                                step_fluc_function=scale_fluc)
    # seed data to make deterministic
    np.random.RandomState(5)

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
    descriptor1_uid = insert_descriptor(
        run_start=runstart_uid, data_keys=data_keys1, time=0.,
        uid=str(uuid.uuid4()))

    descriptor2_uid = insert_descriptor(
        run_start=runstart_uid, data_keys=data_keys2, time=0.,
        uid=str(uuid.uuid4()))

    events = []
    for idx1, i in enumerate(range(num1)):
        img = next(frame_generator)
        img_sum = float(img.sum())
        img_sum_x = img.sum(axis=0)
        img_sum_y = img.sum(axis=1)
        img_x_max = float(img_sum_x.argmax())
        img_y_max = float(img_sum_y.argmax())

        fsid_img = save_ndarray(img)
        fsid_x = save_ndarray(img_sum_x)
        fsid_y = save_ndarray(img_sum_y)

        # Put in actual ndarray data, as broker would do.
        data1 = {'linear_motor': i,
                 'total_img_sum': img_sum,
                 'img': fsid_img,
                 'img_sum_x': fsid_x,
                 'img_sum_y': fsid_y,
                 'img_x_max': img_x_max,
                 'img_y_max': img_y_max
                 }
        timestamps1 = {k: noisy(i) for k in data1}

        event_uid = insert_event(descriptor=descriptor1_uid, seq_num=idx1,
                                 time=noisy(i), data=data1,
                                 timestamps=timestamps1,
                                 uid=str(uuid.uuid4()))
        event, = find_events(uid=event_uid)
        events.append(event)
        for idx2, i2 in enumerate(range(num2)):
            time = noisy(i/num2)
            data2 = {'Tsam': idx1 + np.random.randn()}
            timestamps2 = {'Tsam': time}
            event_uid = insert_event(descriptor=descriptor2_uid,
                                     seq_num=idx2+idx1, time=time, data=data2,
                                     uid=str(uuid.uuid4()),
                                     timestamps=timestamps2)
            event, = find_events(uid=event_uid)
            events.append(event)
        ttime.sleep(sleep)

    return events


def define_parser():
    parser = argparse.ArgumentParser(description='Launch a data viewer')
    parser.add_argument('time', nargs='?', default=0,
                        help="Sleep duration between scan steps")
    return parser

if __name__ == '__main__':
    parser = define_parser()

    args = parser.parse_args()
    sleep_time = float(args.time)

    try:
        last_start_event = next(find_last())
        scan_id = int(last_start_event.scan_id)+1
    except IndexError:
        scan_id = 1
    scan_id = str(scan_id)
    custom = {'plotx': 'linear_motor', 'ploty': ['total_img_sum'],
              'moon': 'full'}
    # insert the run start
    runstart_uid = insert_runstart(scan_id=scan_id, time=ttime.time(),
                                     beamline_id='csx', custom=custom,
                                     uid=str(uuid.uuid4()))
    events = run(runstart_uid=runstart_uid, sleep=sleep_time,
                 make_runstop=False)
    runstop = insert_runstop(run_start=runstart_uid, time=ttime.time(),
                               reason='run completed', exit_status='success',
                               uid=str(uuid.uuid4()))
