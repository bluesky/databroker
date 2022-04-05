import copy
import databroker.databroker
import imp
import os
import pytest
import six
import uuid
import yaml

from bluesky.plans import count
from databroker.utils import ensure_path_exists
from databroker.tests.utils import get_uids
from databroker import (lookup_config, list_configs, describe_configs)
from databroker.v0 import Broker, temp_config


EXAMPLE = {
    "description": "DESCRIPTION_PLACEHOLDER",
    "metadatastore": {
        "module": "databroker.headersource.mongo",
        "class": "MDS",
        "config": {
            "host": "localhost",
            "port": 27017,
            "database": "mds_database_placholder",
            "timezone": "US/Eastern",
        },
    },
    "assets": {
        "module": "databroker.assets.mongo",
        "class": "Registry",
        "config": {
            "host": "localhost",
            "port": 27017,
            "database": "assets_database_placeholder",
        },
    },
    "handlers": {
        "FOO": {"module": "databroker.assets.path_only_handlers", "class": "RawHandler"}
    },
    "root_map": {"foo": "/bar", "boo": "/far"},
}


@pytest.mark.xfail(reason="Not clear why this ever passed; methods are deprecated so follow-up is low prio")
def test_from_config():
    broker = Broker.from_config(EXAMPLE)
    config = broker.get_config()
    assert EXAMPLE == config


def test_handler_registration():
    db = Broker.from_config(EXAMPLE)
    assert 'AD_HDF5' in db.reg.handler_reg  # builtin
    assert 'FOO' in db.reg.handler_reg  # specified by config

    db = Broker.from_config(EXAMPLE, auto_register=False)
    assert 'AD_HDF5' not in db.reg.handler_reg  # builtin
    assert 'FOO' in db.reg.handler_reg  # specified by config


def test_root_map():
    db = Broker.from_config(EXAMPLE)
    assert 'foo' in db.reg.root_map
    assert db.reg.root_map['foo'] == EXAMPLE['root_map']['foo']
    assert db.reg.root_map['boo'] == EXAMPLE['root_map']['boo']


def test_lookup_config():
    name = '__test_lookup_config'
    path = os.path.join(os.path.expanduser('~'), '.config', 'databroker',
                        name + '.yml')
    ensure_path_exists(os.path.dirname(path))
    with open(path, 'w') as f:
        yaml.dump(EXAMPLE, f)
    actual = lookup_config(name)
    broker = Broker.named(name)  # smoke test
    assert broker.name == name
    assert name in list_configs()
    assert name in describe_configs()
    assert describe_configs()[name] == 'DESCRIPTION_PLACEHOLDER'
    os.remove(path)
    assert actual == EXAMPLE

    with pytest.raises(FileNotFoundError):
        lookup_config('__does_not_exist')


def test_uri(RE, hw):

    meta_config = {'uri': 'mongodb://localhost',
                   'database': 'mds_database_placholder',
                   'timezone': 'US/Eastern'}
    asset_config = {'uri': 'mongodb://localhost',
                    'database': 'assets_database_placeholder'}

    config = copy.deepcopy(EXAMPLE)
    config['metadatastore']['config'] = meta_config
    broker = Broker.from_config(config)
    RE.subscribe(broker.insert)
    uid, = get_uids(RE(count([hw.det])))
    run = broker[uid]

    config['api_version'] = 0
    broker = Broker.from_config(config)
