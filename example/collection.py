__author__ = 'arkilic'

from metadataStore.api.collection import save_header, save_beamline_config, save_event, save_event_descriptor
import random
import time
import string


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))



b = save_beamline_config(config_params={'my_beamline': 'my_value'})


ed = save_event_descriptor(event_type_id=1, data_keys=['arm.an', 'arkilic'], descriptor_name=id_generator())

h = save_header(unique_id=str(id_generator(5)), scan_id=3,  create_time=time.time(), beamline_config=b,
                event_descriptor=ed,
                custom={'data':123})