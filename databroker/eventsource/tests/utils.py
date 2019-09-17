from importlib import import_module

from databroker.eventsource.archiver import ArchiverEventSource
from databroker.eventsource.shim import EventSourceShim
from databroker.v0 import Broker

arch_csx_url = 'http://localhost:17668'
arch_csx_pv = 'XF:23ID-ID{BPM}Val:PosXS-I'

t1 = 1475790935.6912444
t2 = 1475790875.0159147


def get_config_for_archiver():
    config = {'name': 'arch_csx',
              'url': arch_csx_url,
              'timezone': 'US/Eastern',
              'pvs': {'pv1': arch_csx_pv}}
    return config


def get_header_for_archiver():
    hdr = {'start': {'time': t1},
           'stop': {'time':  t2}}
    return hdr


def build_es_backed_archiver(config):
    return ArchiverEventSource(config)


def get_db_config_for_metadastore():
    config = {'module': 'databroker.headersource.mongo',
              'class': 'MDS',
              'config': {
                  'host': 'localhost',
                  'port': '27017',
                  'database': 'mds',
                  'timezone': 'US/Eastern'}}
    return config


def get_db_config_for_assets():
    config = {'module': 'databroker.assets.mongo',
              'class': 'Registry',
              'config': {
                  'host': 'localhost',
                  'port': '27017',
                  'database': 'fs'}}
    return config


def get_db_config_for_event_sources():
    arv_config = {'module': 'databroker.eventsource.archiver',
                  'class': 'ArchiverEventSource',
                  'config': get_config_for_archiver()}
    config = []
    config.append(arv_config)
    return config


def get_db_config():
    mds_config = get_db_config_for_metadastore()
    fs_config = get_db_config_for_assets()
    ess_config = get_db_config_for_event_sources()
    config = {'metadatastore': mds_config,
              'assets': fs_config,
              'event_sources': ess_config}
    return config


def load_cls(config):
    modname = config['module']
    clsname = config['class']
    mod = import_module(modname)
    cls = getattr(mod, clsname)
    return cls


def build_mds_from_config():
    config = get_db_config_for_metadastore()
    mds_cls = load_cls(config)
    mds = mds_cls(config['config'])
    return mds


def build_assets_from_config():
    config = get_db_config_for_assets()
    assets_cls = load_cls(config)
    assets = assets_cls(config['config'])
    return assets


def build_event_sources_from_config():
    event_sources = []
    ess = get_db_config_for_event_sources()
    for es in ess:
        es_cls = load_cls(es)
        event_sources.append(es_cls(es['config']))
    return event_sources


def build_shim_from_init():
    mds = build_mds_from_config()
    reg = build_assets_from_config()
    ess = EventSourceShim(mds, reg)
    return ess


def build_db_from_init():
    mds = build_mds_from_config()
    assets = build_assets_from_config()
    event_sources = build_event_sources_from_config()
    db = Broker(mds, assets, event_sources=event_sources)
    return db


def build_db_from_config():
    config = get_db_config()
    db = Broker.from_config(config)
    return db
