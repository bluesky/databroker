import os
import yaml
import logging

logger = logging.getLogger(__name__)

def load_configuration(name, prefix, fields):
    filenames = [os.path.join('etc', name + '.yml'),
                 os.path.join(os.path.expanduser('~'), '.config',
                              name, 'connection.yml'),
                ]
    if 'CONDA_ETC_' in os.environ:
        filenames.insert(0, os.path.join(os.environ['CONDA_ETC_'], name + '.yml'))

    config = {}
    for filename in filenames:
        if os.path.isfile(filename):
            with open(filename) as f:
                config.update(yaml.load(f))
            logger.debug("Using db connection specified in config file. \n%r",
                         config)

    for field in fields:
        var_name = prefix + '_' + field.upper().replace(' ', '_')
        try:
            config[field] = os.environ.get(var_name, config[field])
        except KeyError:
            raise KeyError("The configuration field {0} was not found in any "
                           "file or environmental variable.")
    return config

connection_config = load_configuration('metadatastore', 'MDS',
    ['host', 'database', 'port', 'timezone'])
