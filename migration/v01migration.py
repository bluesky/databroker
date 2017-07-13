from tqdm import tqdm
import ipyparallel as ipp
import argparse



def main(target, source):
    print('Database to be migrated {}'.format(source))
    print('Database migrated to {}'.format(target))
    target = target
    rc = ipp.Client()

    dview = rc[:]

    with dview.sync_imports():
        from metadatastore.mds import MDS, MDSRO
        from collections import deque

    old_config = {
        'database': source,
        'host': 'localhost',
        'port': 27017,
        'timezone': 'US/Eastern'}
    new_config = {
        'database': target,
        'host': 'localhost',
        'port': 27017,
        'timezone': 'US/Eastern'}

    old_t = MDSRO(version=0, config=old_config)
    new_t = MDS(version=1, config=new_config)

    def condition_config():
        import time

        global new, old
        for md in [new, old]:
            md._runstart_col.find_one()
            md._runstop_col.find_one()
            md._event_col.find_one()
            md._descriptor_col.find_one()
        time.sleep(1)

    def invasive_checks():
        global old, new
        return (old._MDSRO__conn is None,
                new._MDSRO__conn is None)

    dview.push({'old': old_t, 'new': new_t})
    dview.apply(condition_config)
    print(list(dview.apply(invasive_checks)))
    new_t._connection.drop_database(target)

    # Drop all indexes on event collection to speed up insert.
    # They will be rebuilt the next time an MDS(RO) object connects.
    new_t._event_col.drop_indexes()
    new = new_t
    old = old_t
    # old._runstart_col.drop_indexes()
    total = old._runstart_col.find().count()
    for start in tqdm(old.find_run_starts(), desc='start docs', total=total):
        new.insert('start', start)

    total = old._runstop_col.find().count()
    for stop in tqdm(old.find_run_stops(), desc='stop docs', total=total):
        try:
            new.insert('stop', stop)
        except RuntimeError:
            print("error inserting run stop with uid {!r}".format(stop['uid']))

    descs = deque()
    counts = deque()
    old._descriptor_col.drop_indexes()
    total = old._descriptor_col.find().count()
    for desc in tqdm(old.find_descriptors(), unit='descriptors', total=total):
        d_raw = next(old._descriptor_col.find({'uid': desc['uid']}))
        num_events = old._event_col.find(
            {'descriptor_id': d_raw['_id']}).count()
        new.insert('descriptor', desc)
        out = dict(desc)
        out['run_start'] = out['run_start']['uid']
        descs.append(dict(desc))
        counts.append(num_events)

    new.clear_process_cache()
    old.clear_process_cache()

    def migrate_event_stream(desc_in, num_events):
        import pymongo.errors
        import time

        global new, old
        if num_events:
            flag = True
            # skip empty event stream of bulk insert raises
            while flag:
                flag = False
                try:
                    events = old.get_events_generator(descriptor=desc_in,
                                                      convert_arrays=False)
                    events = iter(events)
                    l_cache = deque()
                    while True:
                        try:
                            for j in range(5000):
                                l_cache.append(next(events))
                        except StopIteration:
                            break
                        finally:
                            if l_cache:
                                new.bulk_insert_events(descriptor=desc_in,
                                                       events=l_cache)
                            l_cache.clear()
                except KeyError:
                    print("here here, key error")
                except pymongo.errors.AutoReconnect:
                    flag = True
                    time.sleep(10)

        new.clear_process_cache()
        old.clear_process_cache()
        return num_events

    v = rc.load_balanced_view()
    amr = v.map(migrate_event_stream, descs, list(counts), ordered=False)
    total = sum(counts)
    with tqdm(total=total, unit='events') as pbar:
        for res in amr:
            pbar.update(res)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help='v0 database to be migrated')
    parser.add_argument("--target", help='v1 database name for production')
    args = parser.parse_args()
    TARGET = args.target
    V0_DB = args.source  
    ar = main(TARGET, V0_DB)
    print('Done with migration. Verification next')
