from databroker import lookup_config, Broker
from databroker.broker import load_component
import os
import pytest
import six
import yaml


EXAMPLE = {
    'metadatastore': {
        'module': 'databroker.headersource.mongo',
        'class': 'MDSRO',
        'config': {
            'host': 'localhost',
            'port': 27017,
            'database': 'mds_database_placholder',
            'timezone': 'US/Eastern'}
    },
    'assets': {
        'module': 'databroker.assets.mongo',
        'class': 'RegistryRO',
        'config': {
            'host': 'localhost',
            'port': 27017,
            'database': 'assets_database_placeholder'}
    }
}


def test_load_component():
    for config in EXAMPLE.values():
        component = load_component(config)
        assert component.config == config['config']
        assert component.__class__.__name__ == config['class']
        assert component.__module__  == config['module']


def test_from_config():
    Broker.from_config(EXAMPLE)


def test_lookup_config():
    name = '__test_lookup_config'
    path = os.path.join(os.path.expanduser('~'), '.config', 'databroker',
                        name + '.yml')
    with open(path, 'w') as f:
        yaml.dump(EXAMPLE, f)
    actual = lookup_config(name) 
    broker = Broker.named(name)  # smoke test
    os.remove(path)
    assert actual == EXAMPLE

    if six.PY2:
        FileNotFoundError = IOError

    with pytest.raises(FileNotFoundError):
        lookup_config('__does_not_exist')
