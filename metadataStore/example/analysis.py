__author__ = 'arkilic'

from metadataStore.api.analysis import find


print find(scan_id=8878179)
print find(start_time={'start': 142109013, 'end': 1421090219.127196})

print find(scan_id=8878179, start_time={'start': 142109013, 'end': 1421090219.127196})[0].id