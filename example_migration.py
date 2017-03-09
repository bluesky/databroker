from metadatastore.mds import MDSRO
from portable_mds.mongoquery.mds import MDS


source_config = {'host': 'localhost',
                 'port': 27017,
                 'database': 'metadatastore_production_v1',
                 'timezone': 'US/Eastern'}
dest_config = {'directory': 'some_directory',
               'timezone': 'US/Eastern'}

source = MDSRO(source_config)  # a read-only metadatastore object
dest = MDS(dest_config)

for run_start in source.find_run_starts():
    dest.insert_run_start(**run_start)
    for desc in source.find_descriptors(run_start=run_start):
        events = source.get_events_generator(descriptor=desc)
        dest.insert_descriptor(**desc)
        dest.bulk_insert_events(desc, events)
    dest.insert_run_stop(**source.stop_by_start(run_start))
