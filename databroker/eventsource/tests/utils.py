import time

import six

from databroker.eventsource.archiver import ArchiverEventSource

arch_csx_url = 'http://xf23id-ca.cs.nsls2.local:17668'
arch_csx_pv = 'XF:23ID-ID{BPM}Val:PosXS-I'

t1 = 1475790935.6912444
t2 = 1475790875.0159147

def get_config_for_archiver():
    config = {'name' : 'arch_csx',
              'url' : arch_csx_url,
              'timezone' : 'US/Eastern',
              'pvs' : {'pv1': arch_csx_pv}}
    return config

def get_header_for_archiver():
    hdr = {'start' : {'time' : t1},
           'stop' : {'time' :  t2}}
    return hdr

def build_es_backed_archiver(config):
    return ArchiverEventSource(config)
