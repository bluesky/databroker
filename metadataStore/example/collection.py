__author__ = 'arkilic'

from metadataStore.api.collection import save_header, save_beamline_config, save_event, save_event_descriptor
import random
import time
import string


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

h = save_header(scan_id=random.randint(0,10000000),  start_time=time.time(),
                end_time=time.time(), custom={'data':123})
print h.id

b = save_beamline_config(header=h)

ed = save_event_descriptor(header=h, event_type_id=1, data_keys=['arman', 'arkilic'], descriptor_name=id_generator())


save_event(header=h, event_descriptor=ed, seq_no=1, timestamp=time.time(), data={'a': 1, 'bc': 5})