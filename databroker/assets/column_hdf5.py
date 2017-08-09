import h5py
import json
import uuid
import os
import pandas as pd

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

    d_ids = [uuid.uuid4() for j in range(len(dkwargs_table))]
    datum_ids = ['{}/{}'.format(resource_uid, d)
                 for d in d_ids]

    with h5py.File(f'{datum_col}/{resource_uid}.h5', 'w') as fout:
        fout['datum_id'] = [d.encode('utf-8') for d in d_ids]
        for k, v in dkwargs_table.items():
            fout.create_dataset(k, (len(v),),
                                dtype=v.dtype,
                                data=v,
                                maxshape=(None, ))

    return datum_ids


def resource_given_uid(col, res_uid):
    with open(f'{col}/{res_uid}.json', 'r') as fin:
        resource_object = json.load(fin)
    return resource_object


def retrieve(col, datum_id, datum_cache, get_spec_handler, logger):
    r_uid, _, d_uid = datum_id.partition('/')

    handler = get_spec_handler(r_uid)
    try:
        df = datum_cache[r_uid]
    except:
        with h5py.File(f'{col}/{r_uid}.h5', 'r') as fout:
            df = pd.DataFrame({k: fout[k] for k in fout})
            df['datum_id'] = df['datum_id'].str.decode('utf-8')
            df = df.set_index('datum_id')
        datum_cache[r_uid] = df

    return handler(**dict(df.loc[datum_id]))


api = SimpleNamespace(
    insert_resource=insert_resource,
    bulk_register_datum_table=bulk_register_datum_table,
    resource_given_uid=resource_given_uid,
    retrieve=retrieve)


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
