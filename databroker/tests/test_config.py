from databroker import lookup_config, Broker
from databroker.broker import load_component
import databroker.databroker
from databroker.utils import ensure_path_exists
import imp
import os
import pytest
import six
import yaml

if six.PY2:
    FileNotFoundError = IOError


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
    ensure_path_exists(os.path.dirname(path))
    with open(path, 'w') as f:
        yaml.dump(EXAMPLE, f)
    actual = lookup_config(name) 
    broker = Broker.named(name)  # smoke test
    os.remove(path)
    assert actual == EXAMPLE

    with pytest.raises(FileNotFoundError):
        lookup_config('__does_not_exist')


def test_legacy_config():
    name = databroker.databroker.SPECIAL_NAME
    assert 'test' in name
    # assert not hasattr(databroker.databroker, 'DataBroker')

    path = os.path.join(os.path.expanduser('~'), '.config', 'databroker',
                        name + '.yml')

    if os.path.isfile(path):
        os.remove(path)
        # Test config was dirty. We cleaned up for next time, but we cannot
        # recover. Tests must be re-run.
        assert False

    # Since it does not exist, no singleton should be made on import.

    with pytest.raises(AttributeError):
        databroker.databroker.DataBroker

    with pytest.raises(AttributeError):
        databroker.databroker.get_table

    # Now make a working legacy config file.
    ensure_path_exists(os.path.dirname(path))
    with open(path, 'w') as f:
        yaml.dump(EXAMPLE, f)

    # The singleton should be made this time.
    imp.reload(databroker.databroker)
    databroker.databroker.DataBroker
    databroker.databroker.get_table
    imp.reload(databroker)
    from databroker import db, DataBroker, get_table, get_images

    # now make a broken legacy config file.
    broken_example = EXAMPLE.copy()
    broken_example['metadatastore'].pop('module')
    with open(path, 'w') as f:
        yaml.dump(broken_example, f)

    # The singleton should not be made, and it should warn on import
    # about the legacy config being broken.
    with pytest.warns(UserWarning):
        imp.reload(databroker.databroker)

    # Clean up
    os.remove(path)
