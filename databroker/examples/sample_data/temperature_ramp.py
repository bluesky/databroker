from __future__ import division
from metadataStore.api.collection import (insert_event,
                                          insert_event_descriptor)
from ...broker.struct import BrokerStruct
import numpy as np
from . import common

# "Magic numbers" for this simulation
start, stop, step, points_per_step = 0, 3, 1, 10
deadband_size = 0.9
num_exposures = 43

@common.example
def run(begin_run=None):
    # Make the data
    ramp = common.stepped_ramp(start, stop, step, points_per_step)
    deadbanded_ramp = common.apply_deadband(ramp, deadband_size)
    rs = np.random.RandomState(5)
    point_det_data = rs.randn(num_exposures)

    # Create Event Descriptors
    data_keys1 = {'point_det': {'source': 'PV:ES:PointDet'}}
    data_keys2 = {'Tsam': {'source': 'PV:ES:Tsam'}}
    ev_desc1 = insert_event_descriptor(begin_run_event=begin_run,
                                       data_keys=data_keys1, time=0.)
    ev_desc2 = insert_event_descriptor(begin_run_event=begin_run,
                                       data_keys=data_keys2, time=0.)

    # Create Events.
    events = []

    # Point Detector Events
    for i in range(num_exposures):
        time = float(i + 0.01 * rs.randn())
        data = {'point_det': {'value': point_det_data[i], 'timestamp': time}}
        event = insert_event(event_descriptor=ev_desc1, seq_no=i, time=time,
                             data=data)
        events.append(event)

    # Temperature Events
    for i, (time, temp) in enumerate(zip(*deadbanded_ramp)):
        time = float(time)
        data = {'Tsam': {'value': temp, 'timestamp': time}}
        event = insert_event(event_descriptor=ev_desc2, time=time,
                             data=data, seq_no=i)
        events.append(event)

    events = [BrokerStruct(event) for event in events]
    return events
