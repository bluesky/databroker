import time
from databroker.api.data_collection import (
    create_event_descriptor, create_run_header, write_to_event_PV,
    format_event, write_to_hdr_PV)
import random

# header formatting stuff
scan_id = random.randint(0,1000)
header = create_run_header(scan_id=scan_id)

print("run_header: {}".format(header))
# event descriptor formatting stuff
data_keys = ['det', 'mtr']
event_type_id = 1
event_descriptor_name = 'test scan'

event_descriptor = create_event_descriptor(
    header, data_keys=data_keys, event_type_id=event_type_id,
    descriptor_name=event_descriptor_name
)

# write the header and event_descriptor to the header PV
write_to_hdr_PV(header, event_descriptor)

# event formatting stuff
for idx in range(10):
    data = {'det': 100000, 'mtr': 1, 'time': time.time()}
    event = format_event(header, event_descriptor, seq_no=idx, data=data)
    write_to_event_PV(event)
    time.sleep(1)