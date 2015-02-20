from __future__ import division
import uuid
from metadatastore.api import insert_event, insert_event_descriptor
import numpy as np
from dataportal.examples.sample_data import common

# "Magic numbers" for this simulation
start, stop, step, points_per_step = 0, 3, 1, 7
deadband_size = 0.9
num_exposures = 23


@common.example
def run(run_start=None, sleep=0):
    if sleep != 0:
        raise NotImplementedError("A sleep time is not implemented for this "
                                  "example.")
    # Make the data
    ramp = common.stepped_ramp(start, stop, step, points_per_step)
    deadbanded_ramp = common.apply_deadband(ramp, deadband_size)
    rs = np.random.RandomState(5)
    point_det_data = rs.randn(num_exposures)

    # Create Event Descriptors
    data_keys1 = {'point_det': dict(source='PV:ES:PointDet',
                                    dtype='number')}
    data_keys2 = {'Tsam': dict(source='PV:ES:Tsam', dtype='number'),
                  'Troom': dict(source='PV:ES:Troom', dtype='number')}
    ev_desc1 = insert_event_descriptor(run_start=run_start,
                                       data_keys=data_keys1, time=0.,
                                       uid=str(uuid.uuid4()))
    ev_desc2 = insert_event_descriptor(run_start=run_start,
                                       data_keys=data_keys2, time=0.,
                                       uid=str(uuid.uuid4()))

    # Create Events.
    events = []

    # Point Detector Events
    for i in range(num_exposures):
        time = float(i + 0.01 * rs.randn())
        data = {'point_det': (point_det_data[i], time)}
        event = insert_event(event_descriptor=ev_desc1, seq_num=i, time=time,
                             data=data, uid=str(uuid.uuid4()))
        events.append(event)

    # Temperature Events
    for i, (time, temp) in enumerate(zip(*deadbanded_ramp)):
        time = float(time)
        data = {'Tsam': (temp, time),
                'Troom': (temp + 10, time)}
        event = insert_event(event_descriptor=ev_desc2, time=time,
                             data=data, seq_num=i, uid=str(uuid.uuid4()))
        events.append(event)

    return events


if __name__ == '__main__':
    run()
