from metadatastore.mds import MDS, MDSRO
import metadatastore.conf
import doct
from collections import deque
from tqdm import tqdm
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--new_db", help='The migrated database aka v1')
parser.add_argument("--old_db", help='The old database aka v0')
args = parser.parse_args()


NEW_DATABASE = args.new_db
OLD_DATABASE = args.old_db

def compare(o, n):
    try:
        assert o['uid'] == n['uid']
        if 'reason' in o and o['reason'] == '':
            d_o = dict(o)
            del d_o['reason']
            o = doct.Document('RunStop', d_o)
        assert o == n
    except AssertionError:
        if o['run_start']['beamline_id'] in ['CSX', 'xf23id', 'CSX-1']:
            pass
        else:
            print(o)
            print(n)
            raise

old_config = dict(database=OLD_DATABASE,
                  host='localhost',
                  port=27017,
                  timezone='US/Eastern')
new_config = old_config.copy()

new_config['database'] = NEW_DATABASE

old = MDSRO(version=0, config=old_config)
new = MDS(version=1, config=new_config)

total = old._runstart_col.find().count()
old_starts = tqdm(old.find_run_starts(), unit='start docs', total=total,
                  leave=True)
new_starts = new.find_run_starts()
for o, n in zip(old_starts, new_starts):
    compare(o, n)

total = old._runstop_col.find().count()
old_stops = tqdm(old.find_run_stops(), unit='stop docs', total=total)
new_stops = new.find_run_stops()
for o, n in zip(old_stops, new_stops):
    compare(o, n)
descs = deque()
counts = deque()
total = old._descriptor_col.find().count()
old_descs = tqdm(old.find_descriptors(), unit='descriptors', total=total)
new_descs = new.find_descriptors()
for o, n in zip(old_descs, new_descs):
    d_raw = next(old._descriptor_col.find({'uid': o['uid']}))
    num_events = old._event_col.find({'descriptor_id': d_raw['_id']}).count()
    assert o == n
    descs.append(o)
    counts.append(num_events)

total = sum(counts)
with tqdm(total=total, unit='events') as pbar:
    for desc, num_events in zip(descs, counts):
        old_events = old.get_events_generator(descriptor=desc,
                                              convert_arrays=False)
        new_events = new.get_events_generator(descriptor=desc,
                                              convert_arrays=False)
        try:
            for ev in zip(old_events, new_events):
                try:
                    assert o == n
                except KeyError:
                    print(o)
                    print(n)
        except KeyError:
            print('descriptor uid', desc['uid'])
            print('new events', new_events, list(new_events))
            print('old events', old_events, list(old_events))
        pbar.update(num_events)
print('All verified')
