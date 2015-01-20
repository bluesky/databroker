headers = []
beamline_configs = []
event_descriptors = []
events = []


def find2(header_id=None, scan_id=None, owner=None, start_time=None,
          beamline_id=None, end_time=None):
    return {'headers': headers, 'beamline_configs': beamline_configs,
            'event_descriptors': event_descriptors, 'events': events}
