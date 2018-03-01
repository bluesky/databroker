from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import h5py
import os
import pandas as pd
import sqlite3
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path
import itertools
import numpy as np
import hashlib

# from .base_registry import BaseRegistry
from .base_registry import (BaseRegistryRO,
                            RegistryTemplate,
                            RegistryMovingTemplate)
from .sqlite import (ResourceCollection,
                     ResourceUpdatesCollection,
                     RegistryDatabase)
from .core import (resource_given_uid, insert_resource,
                   update_resource, get_resource_history,
                   doc_or_uid_to_uid, get_file_list)
from ..headersource.hdf5 import append
from ..utils import ensure_path_exists as makedirs


class DatumNotFound(Exception):
    pass


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


def make_file_name(datum_path, resource_uid,
                   dir_generations=2, dir_width=2):
    r_uid = resource_uid
    r_hash = hashlib.md5(r_uid.encode('utf-8')).hexdigest()
    dir_levels = [r_hash[dir_width*j:dir_width*(j+1)]
                  for j in range(dir_generations)]
    final_path = os.path.join(datum_path, *dir_levels)
    return final_path, '{}.h5'.format(r_uid)


def bulk_register_datum_table(datum_col,
                              resource_uid, dkwargs_table,
                              validate):
    if validate:
        raise

    path, fname = make_file_name(datum_col, resource_uid)
    makedirs(path, exist_ok=True)
    num_datums = 1
    with h5py.File(os.path.join(path, fname), 'x') as fout:

        for k, v in six.iteritems(dkwargs_table):
            num_datums = len(v)
            fout.create_dataset(k, (len(v),),
                                dtype=v.dtype,
                                data=v,
                                maxshape=(None, ),
                                chunks=True)
        d_ids = np.arange(num_datums)
        fout.create_dataset('datum_id', (num_datums,),
                            dtype=d_ids.dtype,
                            data=d_ids,
                            maxshape=(None, ),
                            chunks=True)

    return ['{}/{}'.format(resource_uid, d) for d in d_ids]


def retrieve(col, datum_id, datum_cache, get_spec_handler, logger):
    if '/' not in datum_id:
        raise DatumNotFound
    r_uid, _, d_uid = datum_id.partition('/')
    d_uid = int(d_uid)
    handler = get_spec_handler(r_uid)
    try:
        df = datum_cache[r_uid]
    except:
        path, fname = make_file_name(col, r_uid)
        with h5py.File(os.path.join(path, fname), 'r') as fin:
            df = pd.DataFrame({k: fin[k][:] for k in fin})
            df = df.set_index('datum_id')
        datum_cache[r_uid] = df

    return handler(**dict(df.loc[d_uid]))


def get_datum_by_res_gen(datum_col, resource_uid):
    path, fname = make_file_name(datum_col, resource_uid)
    fpath = Path(path) / Path(fname)
    if not fpath.is_file():
        return
    with h5py.File(str(fpath), 'r') as fin:
        df = pd.DataFrame({k: fin[k][:] for k in fin})
        df = df.set_index('datum_id')

    for i, r in df.iterrows():
        yield {'datum_id': i,
               'resource': resource_uid,
               'datum_kwargs': dict(r)}


def resource_given_datum_id(col, datum_id, datum_cache, logger):
    r_uid, _, d_uid = datum_id.partition('/')
    return r_uid


def bulk_insert_datum(col, resource, datum_ids,
                      datum_kwarg_list):
    d_uids = bulk_register_datum_table(col,
                                       doc_or_uid_to_uid(resource),
                                       pd.DataFrame(datum_kwarg_list),
                                       False)

    return d_uids


def register_datum(datum_col, resource_uid, datum_kwargs):
    datum = insert_datum(datum_col, resource_uid, None,
                         datum_kwargs, {}, None)

    return datum['datum_id']


def insert_datum(datum_col, resource, datum_id, datum_kwargs,
                 known_spec, resource_col, ignore_duplicate_error=False,
                 duplicate_exc=None):
    resource = doc_or_uid_to_uid(resource)
    if datum_id is not None:
        # Validate that the datum_id we were handed is correct.
        try:
            hopefully_resource, d_id = datum_id.split('/', 1)
            assert hopefully_resource == resource
            int(d_id)
        except Exception:
            raise NotImplementedError("Column insert assumes datum_id "
                                      "has format {resource}/{int}.")
    path, fname = make_file_name(datum_col, resource)
    p = Path(path) / Path(fname)
    # We are transitioning from ophyd objects inserting directly into a
    # Registry to ophyd objects passing documents to the RunEngine which in
    # turn inserts them into a Registry. During the transition period, we allow
    # an ophyd object to attempt BOTH so that configuration files are
    # compatible with both the new model and the old model. Thus, we need to
    # ignore the second attempt to insert.
    if p.is_file():
        # Append to an existing file.
        with h5py.File(str(p), 'a') as fout:
            last = fout['datum_id'][-1]
            cur_max = fout['datum_id'][-1]
            d_id = cur_max + 1  # the integer in datum_id
            datum_id = '{resource}/{d_id}'.format(resource=resource, d_id=d_id)
            if ignore_duplicate_error and d_id <= last:
                # We have seen this already.
                pass
            else:
                assert d_id - last == 1
                append(fout['datum_id'], [d_id])
                for k, v in datum_kwargs.items():
                    append(fout[k], [v])

    else:
        df = pd.DataFrame([datum_kwargs])
        datum_id, = bulk_register_datum_table(datum_col,
                                           resource, df, False)
    return dict(resource=resource,
                datum_id=str(datum_id),
                datum_kwargs=dict(datum_kwargs))


api = SimpleNamespace(
    insert_resource=insert_resource,
    bulk_register_datum_table=bulk_register_datum_table,
    resource_given_uid=resource_given_uid,
    retrieve=retrieve,
    update_resource=update_resource,
    DatumNotFound=DatumNotFound,
    get_resource_history=get_resource_history,
    insert_datum=insert_datum,
    resource_given_datum_id=resource_given_datum_id,
    get_datum_by_res_gen=get_datum_by_res_gen,
    get_file_list=get_file_list,
    bulk_insert_datum=bulk_insert_datum,
    register_datum=register_datum)


class RegistryRO(BaseRegistryRO):

    _API_MAP = {1: api}
    REQ_CONFIG = ('dbpath',)

    def __init__(self, config):
        super(RegistryRO, self).__init__(config)
        makedirs(self.config['dbpath'], exist_ok=True)
        # we are going to be caching dataframes so be
        # smaller!
        self._datum_cache.max_size = 100
        self.__resource_col = None
        self.__db = None
        self.__resource_update_col = None

    @property
    def _datum_col(self):
        return self.config['dbpath']

    @property
    def _db(self):
        if self.__db is None:
            self.__db = RegistryDatabase(self.config['dbpath'] + '/r.sqlite')
        return self.__db

    @property
    def _resource_col(self):
        if self.__resource_col is None:
            self.__resource_col = ResourceCollection(self._db.conn)
        return self.__resource_col

    @property
    def _resource_update_col(self):
        if self.__resource_update_col is None:
            self.__resource_update_col = ResourceUpdatesCollection(
                self._db.conn)
        return self.__resource_update_col

    @property
    def DuplicateKeyError(self):
        return sqlite3.IntegrityError


class Registry(RegistryRO, RegistryTemplate):
    pass


class RegistryMoving(Registry, RegistryMovingTemplate):
    pass
