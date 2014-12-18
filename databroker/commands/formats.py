__author__ = 'edill'

from collections import defaultdict
import six


try:
    import pandas as pd
except ImportError:
    pd = None


def as_object(events):
    ''' Convert the events from mds into an object with data as attributes

    Parameters
    ----------
    events : {dict, list}
        dict if returned from find2 or list if returned from find_last
    '''
    if isinstance(events, dict):
        events = [ev for ev in six.itervalues(events)]
    data_dict = defaultdict(list)
    run_hdr_ids = []
    ev_desc_ids = []
    for e in events:
    #     print(e)
        if 'time' in e:
            data_dict['time'].append(e['time'])
        elif 'time' in e['data']:
            # this will get caught by the loop over data
            pass
        else:
            # not sure if anything else should be done here
            pass

        for data_key, data_val in six.iteritems(e['data']):
            data_dict[data_key].append(data_val)

        run_hdr_ids.append(e['header_id'])
        ev_desc_ids.append(e['descriptor_id'])

    class Event(object):
        pass
    # create the events object
    events_object = Event()
#     setattr(events_object, header_ids, list(set(run_hdr_ids)))
#     setattr(events_object, ev_desc_ids, list(set(ev_desc_ids)))
    # events_object.header_ids =
    # events_object.ev_desc_ids =
    for data_key, data_val in six.iteritems(data_dict):
        setattr(events_object, data_key, data_val)
    return events_object


def as_dataframe(events):
    raise NotImplementedError('Conversion to pandas dataframe not supported')

