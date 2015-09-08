__author__ = 'arkilic'
import jsonschema
import ujson
from enum import Enum
from pkg_resources import resource_filename as rs_fn


class DocumentNames(Enum):
    stop = 'stop'
    start = 'start'
    descriptor = 'descriptor'
    event = 'event'

SCHEMA_PATH = 'schema'
SCHEMA_NAMES = {DocumentNames.start: 'run_start.json',
                DocumentNames.stop: 'run_stop.json',
                DocumentNames.event: 'event.json',
                DocumentNames.descriptor: 'event_descriptor.json'}
fn = '{}/{{}}'.format(SCHEMA_PATH)
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    rn=fn.format(filename)
    print(rn)
    with open(rs_fn('metadataservice', resource_name=rn)) as fin:
        print(ujson.load(fin))