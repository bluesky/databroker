from ._dummies import _events


def find(header_id=None, scan_id=None, owner=None, start_time=None,
         beamline_id=None, end_time=None):
    return list(_events.values())
