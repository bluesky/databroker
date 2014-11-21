import time
import json

from epics import PV

from metadataStore.api.collection import create_event
from pprint import pprint

header_PV_name = "XF:23ID-CT{Replay}Val:RunHdr-I"
event_PV_name = "XF:23ID-CT{Replay}Val:EventHdr-I"


def header_callback(value, **kw):
    """
    Header and event descriptor should already be created by the data
    broker before they are published to the header PV, as this callback will
    not create PVs.

    :param value:
    :param kw:
    :return:
    """
    raw_data = kw['char_value']
    print raw_data
    data = json.loads(raw_data)
    try:
        header = data['header']
        print("\nNew Header Received"
              "\n===================")
        pprint(header)
    except KeyError as ke:
        print('...but there is no run header present')
    try:
        event_descriptors = data['event_descriptors']
        num_ev_desc = 1
        if (isinstance(event_descriptors, list)
            or isinstance(event_descriptors, tuple)):
            num_ev_desc = len(event_descriptors)
        else:
            num_ev_desc = 1
            event_descriptors = [event_descriptors, ]
        print("\nNew Event Descriptor(s) Received"
              "\n================================")
        for idx, ev_desc in enumerate(event_descriptors):
            print('ev_desc {}'
                  '\n----------'.format(idx))
            pprint(ev_desc)
    except KeyError as ke:
        print('No event descriptor present')


def event_callback(value, **kw):
    raw_data = kw['char_value']
    data = json.loads(raw_data)
    create_event(data)
    print("\nNew Event Received"
          "\n==================")
    pprint(data)


def start():
    PV_hdr = PV(header_PV_name, auto_monitor=True)
    PV_hdr.add_callback(header_callback)

    PV_event = PV(event_PV_name, auto_monitor=True)
    PV_event.add_callback(event_callback)

    print 'Now wait for changes'

    t0 = time.time()
    #while time.time()-t0 < 300:
    while True:
        time.sleep(0.01)


    print 'Broker stopped!'

if __name__ == '__main__':
    start()