from __future__ import division
from metadataStore.api.collection import (insert_event,
                                          insert_event_descriptor)
from ...broker.struct import BrokerStruct
import numpy as np
import common

# "Magic numbers" for this simulation
start, stop, step = 0, 10, 1
deadband_size = 0.9
num_exposures = 43

@common.example
def run(begin_run=None):
    # Make the data
    ramp = common.stepped_ramp(start, stop, step)
    deadbanded_ramp = common.apply_deadband(ramp, deadband_size)
    point_det_data = point_det = np.random.randn(num_exposures)

    # Create Event Descriptors
    data_keys1 = {'point_det': {'source': 'PV:ES:PointDet'}}
    data_keys2 = {'Tsam': {'source': 'PV:ES:Tsam'}}
    ev_desc1 = insert_event_descriptor(begin_run_event=begin_run,
                                       data_keys=data_keys1, time=0.)
    ev_desc2 = insert_event_descriptor(begin_run_event=begin_run,
                                       data_keys=data_keys2, time=0.)

    # Create Events.
    events = []

    # Temperature Events
    for i in range(num_exposures):
        time = float(i)  # seconds since 1970; must be a float
        data = {'point_det': {'value': point_det, 'timestamp': time}}
        event = insert_event(event_descriptor=ev_desc1, seq_no=i, time=time,
                             data=data)
        events.append(event)

    # Point Detector Events
    # We will stretch the time index so that the durations roughly match.
    point_det_run_time = num_exposures
    temp_run_time = 1./deadbanded_ramp[-1][0]
    for time, temp in zip(*deadbanded_ramp):
        stretched_time = float(point_det_run_time/temp_run_time * time)
        data = {'Tsam': {'value': temp, 'timestamp': stretched_time}}
        event = insert_event(event_descriptor=ev_desc2, time=stretched_time,
                             data=data, seq_no=i)
        events.append(event)

        events = [BrokerStruct(event) for event in events]
        return events
