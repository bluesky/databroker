import os
import yaml
import logging

logger = logging.getLogger(__name__)
filename = os.path.join(os.path.expanduser('~'), '.config', 'metadatastore',
                        'connection.yml')
if os.path.isfile(filename):
    with open(filename) as f:
        connection_config = yaml.load(f)
    logger.debug("Using db connection specified in config file. \n%r",
                 connection_config)
else:
    connection_config = {
        'database': 'test',
        'host': "localhost",
        'port': 27017,
        }
    logger.debug("Using default db connection. \n%r",
                 connection_config)
