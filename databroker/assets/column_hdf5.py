import h5py
import json
import uuid
import os

# from .base_registry import BaseRegistry
from databroker.assets.base_registry import RegistryTemplate

try:
    from types import SimpleNamespace
except ImportError:
    # LPy compatibility
    class SimpleNamespace:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def __repr__(self):
            keys = sorted(self.__dict__)
            items = ("{}={!r}".format(k, self.__dict__[k]) for k in keys)
            return "{}({})".format(type(self).__name__, ", ".join(items))

        def __eq__(self, other):
            return self.__dict__ == other.__dict__


def insert_resource(col, spec, rpath, rkwargs,
                    known_spec, root, path_semantics):
    resource_kwargs = dict(rkwargs)
    res_uid = str(uuid.uuid4())
    resource_object = dict(spec=str(spec),
                           resource_path=str(rpath),
                           root=str(root),
                           resource_kwargs=resource_kwargs,
                           uid=res_uid,
                           path_semantics=path_semantics)
    with open(f'{col}/{res_uid}.json', 'w') as fout:
        json.dump(resource_object, fout)
    return resource_object


def bulk_register_datum_table(datum_col, resource_col,
                              resource_uid, dkwargs_table,
                              validate):
    if validate:
        raise

    datum_uids = ['{}/{}'.format(resource_uid, uuid.uuid4())
                  for j in range(len(dkwargs_table))]

    with h5py.File(f'{datum_col}/{resource_uid}.h5', 'w') as fout:
        fout['datum_uid'] = [d.encode('utf-8') for d in datum_uids]
        for k, v in dkwargs_table.items():
            fout.create_dataset(k, (len(v),),
                                dtype=v.dtype,
                                data=v,
                                maxshape=(None, ))

    return datum_uids


api = SimpleNamespace(
    insert_resource=insert_resource,
    bulk_register_datum_table=bulk_register_datum_table)


class ColumnHdf5Registry(RegistryTemplate):

    _API_MAP = {1: api}
    REQ_CONFIG = ('dbpath',)

    def __init__(self, config):
        super().__init__(config)
        os.makedirs(self.config['dbpath'], exist_ok=True)
        # we are going to be caching dataframes so be
        # smaller!
        self._datum_cache.max_size = 100

    @property
    def _resource_col(self):
        return self.config['dbpath']

    @property
    def _datum_col(self):
        return self.config['dbpath']
