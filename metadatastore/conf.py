import os
import yaml
import logging

logger = logging.getLogger(__name__)


def load_configuration(name, prefix, fields):
    """
    Load configuration data from a cascading series of locations.

    The precedence order is (highest priority last):

    1. The conda environment
       - CONDA_ENV/etc/{name}.yaml (if CONDA_ETC_ is defined for the env)
    2. At the system level
       - /etc/{name}.yml
    3. In the user's home directory
       - ~/.config/{name}/connection.yml
    4. Environmental variables
       - {PREFIX}_{FIELD}

    where
        {name} is metadatastore
        {PREFIX} is MDS
        {FIELD} is one of {host, database, port, timezone}

    Parameters
    ----------
    name : str
        The expected base-name of the configuration files

    prefix : str
        The prefix when looking for environmental variables

    fields : iterable of strings
        The required configuration fields

    Returns
    ------
    conf : dict
        Dictionary keyed on ``fields`` with the values extracted
    """
    filenames = [
        os.path.join('/etc', name + '.yml'),
        os.path.join(os.path.expanduser('~'), '.config', name,
                     'connection.yml'),
        ]

    if 'CONDA_ETC_' in os.environ:
        filenames.insert(0, os.path.join(
            os.environ['CONDA_ETC_'], name + '.yml'))

    config = {}
    for filename in filenames:
        if os.path.isfile(filename):
            with open(filename) as f:
                config.update(yaml.load(f))
            logger.debug("Using db connection specified in config file. \n%r",
                         config)

    for field in fields:
        var_name = prefix + '_' + field.upper().replace(' ', '_')
        config[field] = os.environ.get(var_name, config.get(field, None))
        # Valid values for 'port' are None or castable to int.
        if field == 'port' and config[field] is not None:
            config[field] = int(config[field])

    missing = [k for k, v in config.items() if v is None]
    if missing:
        raise KeyError("The configuration field(s) {0} "
                       "were not found in any file or environmental "
                       "variable.".format(missing))
    return config

connection_config = load_configuration(
    'metadatastore', 'MDS', ['host', 'database', 'port', 'timezone'])
