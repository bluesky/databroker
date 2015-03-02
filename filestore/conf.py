import os
import uuid
import yaml
import logging

logger = logging.getLogger(__name__)

connection_config = None


def read_connection_config(filename=None):
    """
    Re-read the connection.yml file

    If mongoengine is connected, you must call db_disconnect before the
    new values will be respected.
    """
    global connection_config
    if filename is None:
        filename = os.path.join(os.path.expanduser('~'), '.config',
                                'filestore',
                                'connection.yml')

    if os.path.isfile(filename):
        with open(filename) as f:
            connection_config = yaml.load(f)
        logger.debug("Using db connection specified in config file. \n%r\n%r",
                     filename, connection_config)
    else:
        connection_config = {
            'database': 'test-{0}'.format(uuid.uuid4()),
            'host': "localhost",
            'port': 27017,
            }
        logger.debug("Using default db connection. \n%r",
                     connection_config)


read_connection_config()
