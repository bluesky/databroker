__author__ = 'arkilic'

from metadataStore.api.collection import save_begin_run, save_beamline_config, save_event, save_event_descriptor
import random
import time
import string
import uuid


"""
DO NOT COMPLAIN ABOUT C++-like commented documentation here. I will get rid off it once everybody knows what this
refactor was all about.
"""

#######################################################################################################################
# After the current changes to the database schema, EventDescriptor and BeamlineConfig has to be
# created before headers. This is due to the fact that there is a one to many relationship between EventDescriptors
# and Headers.In other words, since we have a limited set of EventDescriptors that serve for specific purposes(see doc)
# they have to be available to be "referred" by the headers. See the below code for a sequence example
#
# Also, notice that we no longer have unique scan_id. This is due to the fact that, now we have a unique_id field
# that will be hashed by opyhd(convention in progress, talk to Daron and Stuart).
#
######################################################################################################################

b_config = save_beamline_config(config_params={'my_beamline': 'my_value'})


begin_event = save_begin_run(scan_id=3,
                             time=time.time(), beamline_config=b_config,
                             beamline_id='csx', custom={'data': 123})

e_desc = save_event_descriptor(begin_run_event=begin_event,
                               data_keys=['arm.an', 'arkilic'])

e_desc2 = save_event_descriptor(begin_run_event=begin_event,
                                data_keys=['arm.an', 'arkilic'])

e = save_event(begin_run_event=begin_event, event_descriptor=e_desc,
               seq_no=1, beamline_id='csx',
               time=time.time(), data={'arm.an': 1, 'arkilic': 5})