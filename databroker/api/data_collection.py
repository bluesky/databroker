__author__ = 'edill'

from metadataStore.api.collection import find as _find
from metadataStore.api.collection import create as _create
from metadataStore.api.collection import record as _record

def check_scan_id(scan_id, **kwargs):
    # fixme
    return scan_id

def compose_header(scan_id, **kwargs):

    scan_id = check_scan_id(scan_id, kwargs)
    # parse kwargs
    return {}

def compose_ev_desc(run_hdr, ev_type_id, **kwargs):

    # do stuff
    return {}

def compose_event(run_hdr, ev_desc, **kwargs):

    # do stuff
    return {}