from __future__ import division
import uuid
from metadataclient.api import (insert_event, insert_descriptor,
                               find_events)
import numpy as np
from metadataclient.sample_data import common

# "Magic numbers" for this simulation
start, stop, step, points_per_step = 0, 3, 1, 7
deadband_size = 0.9


@common.example
def run(run_start=None, sleep=0):
    if sleep != 0:
        raise NotImplementedError("A sleep time is not implemented for this "
                                  "example.")
    # Make the data
    ramp = common.stepped_ramp(start, stop, step, points_per_step)
    deadbanded_ramp = common.apply_deadband(ramp, deadband_size)

    # Create Event Descriptors
    data_keys = {'Tsam': dict(source='PV:ES:Tsam', dtype='number', shape=[]),
                 'point_det': dict(source='PV:ES:point_det', dtype='number', shape=[])}

    ev_desc = insert_descriptor(run_start=run_start,
                                      data_keys=data_keys, time=0.,
                                      uid=str(uuid.uuid4()))


    # Create Events.
    events = []

    # Temperature Events
    for i, (time, temp) in enumerate(zip(*deadbanded_ramp)):
        time = float(time)
        point_det = np.random.randn()
        data = {'Tsam': temp, 'point_det': point_det}
        timestamps = {'Tsam': time, 'point_det': time}
        event_uid = insert_event(descriptor=ev_desc, time=time, data=data,
                                 seq_num=i, timestamps=timestamps,
                                 uid=str(uuid.uuid4()))

        event, = find_events(uid=event_uid)
        events.append(event)
    print(events)
    return events


if __name__ == '__main__':
    run()