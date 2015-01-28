__author__ = 'arkilic'

from metadataStore.api.collection import (save_header, save_beamline_config,
                                          save_event, save_event_descriptor)
from metadataStore.api.analysis import find_last
import random
import time
import string
import numpy as np

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


"""
DO NOT COMPLAIN ABOUT C++-like commented documentation here. I will get rid off it once everybody knows what this
refactor was all about.
"""

#######################################################################################################################
# After the current changes to the database schema, EventDescriptor and BeamlineConfig has to be
# created before headers. This is due to the fact that there is a one to many relationship between EventDescriptors
# and Headers.In other words, since we have a limited set of EventDescriptors that serve for specific purposes(see doc)
# they have to be available to be "referred" by the headers. See the below code for a sequence example
#
# Also, notice that we no longer have unique scan_id. This is due to the fact that, now we have a unique_id field
# that will be hashed by opyhd(convention in progress, talk to Daron and Stuart).
#
######################################################################################################################

b_config = save_beamline_config(config_params={'my_beamline': 'my_value'})


data_keys = {'linear_motor': 'PV1', 'scalar_detector': 'PV2', 'Tsam': 'PV3'}

e_desc = save_event_descriptor(event_type_id=1, data_keys=data_keys,
                               descriptor_name=id_generator())

try:
    last_hdr = find_last()
    scan_id = last_hdr.scan_id+1
except IndexError:
    scan_id = 1

h = save_header(unique_id=str(id_generator(5)), scan_id=scan_id,
                create_time=time.time(), beamline_config=b_config,
                event_descriptors=[e_desc])

func = np.cos
num = 1000
start = 0
stop = 10

for idx, i in enumerate(np.linspace(start, stop, num)):
    data = {'linear_motor': i,
            'Tsam': i + 5,
            'scalar_detector': func(i)}
    e = save_event(header=h, event_descriptor=e_desc, seq_no=idx,
                   beamline_id='csx', timestamp=time.time(),
                   data=data)

print find_last()