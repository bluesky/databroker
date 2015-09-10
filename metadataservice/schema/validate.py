from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from pkg_resources import resource_filename as rs_fn
import ujson


SCHEMA_PATH = 'schema'
SCHEMA_NAMES = {'run_start': 'run_start.json',
                'run_stop': 'run_stop.json',
                'event': 'event.json',
                'descriptor': 'event_descriptor.json'}
fn = '{}/{{}}'.format(SCHEMA_PATH)
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    with open(rs_fn('metadataservice', resource_name=fn.format(filename))) as fin:
        schemas[name] = ujson.load(fin)