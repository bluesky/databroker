from __future__ import division
import uuid

import numpy as np
from metadatastore.examples.sample_data import common

# "Magic numbers" for this simulation
start, stop, step, points_per_step = 0, 6, 1, 7
deadband_size = 0.9
num_exposures = 17


@common.example
def run(mds, run_start_uid=None, sleep=0):
    if sleep != 0:
        raise NotImplementedError("A sleep time is not implemented for this "
                                  "example.")
    # Make the data
    ramp = common.stepped_ramp(start, stop, step, points_per_step)
    deadbanded_ramp = common.apply_deadband(ramp, deadband_size)
    rs = np.random.RandomState(5)
    point_det_data = rs.randn(num_exposures) + np.arange(num_exposures)

    # Create Event Descriptors
    data_keys1 = {'point_det': dict(source='PV:ES:PointDet', dtype='number'),
                  'boolean_det': dict(source='PV:ES:IntensityDet', dtype='string'),
                  'ccd_det_info': dict(source='PV:ES:CCDDet', dtype='list')}
    data_keys2 = {'Tsam': dict(source='PV:ES:Tsam', dtype='number')}
    ev_desc1_uid = mds.insert_descriptor(run_start=run_start_uid,
                                         data_keys=data_keys1,
                                         time=common.get_time(),
                                         uid=str(uuid.uuid4()),
                                         name='primary')
    ev_desc2_uid = mds.insert_descriptor(run_start=run_start_uid,
                                         data_keys=data_keys2,
                                         time=common.get_time(),
                                         uid=str(uuid.uuid4()),
                                         name='baseline')

    # Create Events.
    events = []

    # Point Detector Events
    base_time = common.get_time()
    for i in range(num_exposures):
        time = float(2 * i + 0.5 * rs.randn()) + base_time
        data = {'point_det': point_det_data[i],
                'boolean_det': 'Yes',
                'ccd_det_info': ['on', 'off']}
        timestamps = {'point_det': time,
                      'boolean_det': time,
                      'ccd_det_info': time}
        event_dict = dict(descriptor=ev_desc1_uid, seq_num=i,
                          time=time, data=data, timestamps=timestamps,
                          uid=str(uuid.uuid4()))
        event_uid = mds.insert_event(**event_dict)
        # grab the actual event from metadatastore
        event, = mds.find_events(uid=event_uid)
        events.append(event)
        assert event['data'] == event_dict['data']

    # Temperature Events
    for i, (time, temp) in enumerate(zip(*deadbanded_ramp)):
        time = float(time) + base_time
        data = {'Tsam': temp}
        timestamps = {'Tsam': time}
        event_dict = dict(descriptor=ev_desc2_uid, time=time,
                          data=data, timestamps=timestamps, seq_num=i,
                          uid=str(uuid.uuid4()))
        event_uid = mds.insert_event(**event_dict)
        event, = mds.find_events(uid=event_uid)
        events.append(event)
        assert event['data'] == event_dict['data']

    return events
