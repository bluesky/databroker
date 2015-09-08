import ujson
from pkg_resources import resource_filename as rs_fn
__author__ = 'arkilic'

SCHEMA_PATH = 'schema'
SCHEMA_NAMES = {'run_start': 'run_start.json',
                'run_stop': 'run_stop.json',
                'event': 'event.json',
                'descriptor': 'event_descriptor.json'}
fn = '{}/{{}}'.format(SCHEMA_PATH)
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    rn=fn.format(filename)
    with open(rs_fn('metadataservice', resource_name=rn)) as fin:
        schemas[name] = ujson.load(fin)