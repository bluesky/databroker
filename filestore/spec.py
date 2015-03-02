from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import logging
import json
from pkg_resources import resource_filename
logger = logging.getLogger(__name__)

known_spec = dict()

for spec_name in ['AD_HDF5', 'AD_SPE']:
    tmp_dict = {}
    resource_name = 'json/{}_resource.json'.format(spec_name)
    datum_name = 'json/{}_datum.json'.format(spec_name)
    print(resource_filename('filestore', resource_name))
    print(datum_name)
    with open(resource_filename('filestore', resource_name), 'r') as fin:
        tmp_dict['resource'] = json.load(fin)
    with open(resource_filename('filestore', datum_name), 'r') as fin:
        tmp_dict['datum'] = json.load(fin)
    known_spec[spec_name] = tmp_dict
