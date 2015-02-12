import os
import yaml
import logging

logger = logging.getLogger(__name__)
filename = os.path.join(os.path.expanduser('~'), '.config', 'metadatastore',
                        'connection.yml')
if os.path.isfile(filename):
    with open(filename) as f:
        mds_config = yaml.load(f)
    logger.debug("Using db connection specified in config file.")
else:
    logger.debug("Using default db connection.")
    mds_config = {
        'database': 'metadataStore',
        'host': "localhost",
        'port': 27017,
        }
