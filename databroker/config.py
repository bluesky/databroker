import os
import yaml
import logging

logger = logging.getLogger(__name__)


def load_configuration(name, prefix, fields, fname=None):
    """
    Load configuration data form a cascading series of locations.

    The precedence order is (highest priority last):

    1. CONDA_ENV/etc/{name}.yml (if CONDA_ETC_ env is defined)
    2. /etc/{name}.yml
    3. ~/.config/{name}/connection.yml
    4. reading {PREFIX}_{FIELD} environmental variables
    5. reading from fname (if it exists)

    Parameters
    ----------
    name : str
        The expected base-name of the configuration files

    prefix : str
        The prefix when looking for environmental variables

    fields : iterable of strings
        The required configuration fields

    fname : str, optional
        Filepath to ultimate configuration file.

    Returns
    ------
    conf : dict
        Dictionary keyed on ``fields`` with the values extracted
    """
    class _PH:
        pass

    filenames = [
        os.path.join('/etc', name + '.yml'),
        os.path.join(os.path.expanduser('~'), '.config',
                     name, 'connection.yml'),
        ]
    if 'CONDA_ETC_' in os.environ:
        filenames.insert(0, os.path.join(os.environ['CONDA_ETC_'],
                                         name + '.yml'))

    config = {}
    for filename in filenames:
        if os.path.isfile(filename):
            with open(filename) as f:
                config.update(yaml.load(f))
            logger.debug("Using db connection specified in config file. \n%r",
                         config)

    config = {k: v for k, v in config.items() if k in fields}

    for field in fields:
        var_name = prefix + '_' + field.upper().replace(' ', '_')

        config[field] = os.environ.get(var_name, config.get(field, _PH))
        if field == 'port':
            if config[field] is not None and config[field] is not _PH:
                config[field] = int(config[field])

    if fname is not None:
        if os.path.isfile(fname):
            with open(fname) as f:
                config.update(yaml.load(f))
            logger.debug("Using db connection specified in config file. \n%r",
                         config)

    missing = [k for k, v in config.items() if v is _PH]
    if missing:
        raise KeyError("The configuration field(s) {0} were not found in any "
                       "file or environmental variable.".format(missing))

    return config
