__author__ = ['arkilic', 'tcaswell'] 
# I am not solely responsible if this cocks up. Tom was here too...

from mongoengine import connect
import mongoengine.connection 
from pymongo import MongoClient
import six
from metadatastore.api import insert_beamline_config, insert_run_start, insert_run_stop, insert_event_descriptor, insert_event
import metadatastore.conf as conf

conf.mds_config['database'] =  'datastore2'

import mongoengine

connect(db='datastore2', host='xf23id-broker', port=27017)
assert mongoengine.connection.get_db().name == 'datastore2'

client = MongoClient(host='xf23id-broker', port=27017)
db = client.toBemigrated1
beamline_cfg_mapping = dict()


beamline_configs = db.beamline_config.find()
for bc in beamline_configs:
    bcfg_id = bc['_id']
    the_bc = insert_beamline_config(config_params=bc['config_params'], time=bc['time'])
    beamline_cfg_mapping[bc['_id']] = the_bc    


begin_runs = db.begin_run_event.find()
for br in begin_runs:
    the_run_start = insert_run_start(time=br['time'], beamline_id=br['beamline_id'], beamline_config=the_bc, owner=br['owner'],
                                     scan_id=br['scan_id'], custom=br.get('custom',{}), uid=br['uid'])
    event_descs = db.event_descriptor.find({'begin_run_id': br['_id']})
    max_time = 0.0     
    for e_desc in event_descs:
        the_e_desc = insert_event_descriptor(run_start=the_run_start, data_keys=e_desc['data_keys'], 
                                             time=e_desc['time'], uid=e_desc['uid'])
        events = db.event.find({'descriptor_id': e_desc['_id']})
        for ev in events:
            if ev['time'] > max_time:
                max_time = ev['time']
            insert_event(event_descriptor=the_e_desc, time=ev['time'], data=ev['data'], seq_num=ev['seq_num'], uid=ev['uid'])
    insert_run_stop(run_start=the_run_start, time=float(max_time), exit_status='success',
                    reason=None, uid=None)

run_start_mapping = dict()
run_starts = db.run_start.find()
for rs in run_starts:
    time = rs.pop('time')
    beamline_id = rs.pop('beamline_id')
    bcfg_id = beamline_cfg_mapping[rs.pop('beamline_config_id')]
    owner = rs.pop('owner')
    scan_id = rs.pop('scan_id')
    uid = rs.pop('uid')
    trashed = rs.pop('time_as_datetime')
    my_run_start = insert_run_start(time= time, beamline_id= beamline_id,
                                    beamline_config=bcfg_id, owner=owner, 
                                    scan_id=scan_id, uid=uid) 
    run_start_mapping[rs['_id']] = my_run_start

    e_descs = db.event_descriptor.find({'run_start_id': rs['_id'] })
    for e_d in e_descs:
         my_e_d = insert_event_descriptor(run_start=my_run_start, data_keys=e_d['data_keys'],
                                          time=e_d['time'], uid=e_d['uid'])
         ev_s = db.event.find({'descriptor_id': e_d['_id']})
         for e in ev_s:
            insert_event(event_descriptor=my_e_d, time=e['time'], data=e['data'], seq_num=e['seq_num'], uid=e['uid'])

end_runs = db.end_run.find({'run_start_id': rs})
for er in end_runs:
    rsta = run_start_mapping.pop(er['run_start_id'])
    insert_run_stop(run_start=rsta, time=er['time'], exit_status=er['exit_status'],
                    reason=er['reason'], uid=er['uid'])
for v in six.itervalues(run_start_mapping):
   insert_run_stop(run_start=v, time=v.time, exit_status='success',
                      reason=None, uid=None)  
