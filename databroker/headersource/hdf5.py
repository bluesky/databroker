import os
import h5py
import numpy as np
from collections import defaultdict
from .mongoquery import JSONCollection
from .base import MDSTemplate, MDSROTemplate
from ..utils import ensure_path_exists


def append(dataset, data):
    data = np.asanyarray(data)
    cur_shape = dataset.shape
    num_events = data.shape[0]
    new_shape = (dataset.shape[0] + num_events,) + cur_shape[1:]
    dataset.resize(new_shape)
    dataset[cur_shape[0]:, ...] = data


class RunStartCollection(JSONCollection):
    def __init__(self, event_col, *args, **kwargs):
        self._event_col = event_col
        super(RunStartCollection, self).__init__(*args, **kwargs)

    def insert_one(self, doc):
        self._event_col.new_runstart(doc)
        super(RunStartCollection, self).insert_one(doc, fk='uid')


class DescriptorCollection(JSONCollection):
    def __init__(self, event_col, *args, **kwargs):
        self._event_col = event_col
        super(DescriptorCollection, self).__init__(*args, **kwargs)

    def insert_one(self, doc):
        self._event_col.new_descriptor(doc)
        super(DescriptorCollection, self).insert_one(doc)


class EventCollection(object):
    def __init__(self, dirpath):
        self._runstarts = {}
        self._descriptors = {}
        self._dirpath = dirpath

    def new_runstart(self, doc):
        uid = doc['uid']
        fp = os.path.join(self._dirpath, '{}.h5'.format(uid))
        self._runstarts[uid] = fp

    def new_descriptor(self, doc):
        uid = doc['uid']
        run_start_uid = doc['run_start']
        groupname = 'desc_' + uid.replace('-', '_')
        fp = self._runstarts[run_start_uid]
        with h5py.File(fp, 'a') as f:
            g = f.create_group(groupname)
            g.create_dataset('uid', shape=(0,), maxshape=(None,), dtype='S36')
            g.create_dataset('time', shape=(0,), maxshape=(None,),
                             dtype='float64')
            g.create_dataset('seq_num', shape=(0,), maxshape=(None,))
            g.create_group('data')
            g.create_group('timestamps')
            for key in doc['data_keys']:
                # Create an empty, resizable dataset for each key.
                g['timestamps'].create_dataset(key, shape=(0,),
                                               maxshape=(None,),
                                               dtype='float64')
        self._descriptors[uid] = run_start_uid

    def find(self, query, sort=None):
        if list(query.keys()) != ['descriptor']:
            raise NotImplementedError("Only queries based on descriptor uid "
                                      "are supported.")
        desc_uid = query['descriptor']
        groupname = 'desc_' + desc_uid.replace('-', '_')
        fp = self._runstarts[self._descriptors[desc_uid]]
        with h5py.File(fp, 'r') as f:
            g = f[groupname]
            transposed_uid = list(g['uid'][:])
            transposed_seq_num = list(g['seq_num'][:])
            transposed_time = list(g['time'][:])
            transposed_data = {}
            transposed_ts = {}
            for key in g['data']:
                transposed_data[key] = list(g['data'][key][:])
                transposed_ts[key] = list(g['timestamps'][key][:])
        for _ in range(len(transposed_uid)):
            event = {}
            event['uid'] = transposed_uid.pop(0).decode()
            event['seq_num'] = transposed_seq_num.pop(0)
            event['time'] = transposed_time.pop(0)
            event['data'] = {}
            event['timestamps'] = {}
            for key in transposed_data:
                data = transposed_data[key].pop(0)
                if data.dtype.kind == 'S':
                    data = bytes(data).decode('utf-8')
                event['data'][key] = data
                event['timestamps'][key] = transposed_ts[key].pop(0)
            yield event

    def find_one(self, query):
        # not used on event_col
        raise NotImplementedError()

    def insert_one(self, doc):
        # not used on event_col
        return self.insert([doc])

    def insert(self, docs):
        # Sort docs by descriptor.
        descs = defaultdict(list)
        for doc in docs:
            descs[doc['descriptor']].append(doc)
        # Process one descriptor at a time.
        for uid, docs in descs.items():
            keys = docs[0]['data']
            transposed_data = {k: [] for k in keys}
            transposed_ts = {k: [] for k in keys}
            transposed_uid = []
            transposed_seq_num = []
            transposed_time = []
            for doc in docs:
                transposed_uid.append(doc['uid'])
                transposed_time.append(doc['time'])
                transposed_seq_num.append(doc['seq_num'])
                data = doc['data']
                ts = doc['timestamps']
                for k in keys:
                    transposed_data[k].append(data[k])
                    transposed_ts[k].append(ts[k])
            fp = self._runstarts[self._descriptors[uid]]
            groupname = 'desc_' + uid.replace('-', '_')
            with h5py.File(fp, 'a') as f:
                g = f[groupname]
                uids = [s.encode('ascii', 'ignore') for s in transposed_uid]
                append(g['uid'], uids)
                append(g['time'], transposed_time)
                append(g['seq_num'], transposed_seq_num)
                for k in keys:
                    data = np.asarray(transposed_data[k])
                    dtype = data.dtype
                    if dtype.kind == 'U':
                        data = data.astype('S')
                        dtype = data.dtype
                    if k not in g['data']:
                        shape = np.asarray(data).shape[1:]
                        g['data'].create_dataset(k, shape=(0,) + shape,
                                                 maxshape=(None,) + shape,
                                                 dtype=dtype)

                    append(g['data'][k], data)
                    append(g['timestamps'][k], transposed_ts[k])


class _CollectionMixin(object):
    def __init__(self, *args, **kwargs):
        self._config = None
        super(_CollectionMixin, self).__init__(*args, **kwargs)
        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None
        ensure_path_exists(self._config['directory'])

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, val):
        self._config = val
        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None

    @property
    def _runstart_col(self):
        if self.__runstart_col is None:
            fp = os.path.join(self.config['directory'], 'run_starts.json')
            self.__runstart_col = RunStartCollection(self._event_col, fp)
        return self.__runstart_col

    @property
    def _runstop_col(self):
        if self.__runstop_col is None:
            fp = os.path.join(self.config['directory'], 'run_stops.json')
            self.__runstop_col = JSONCollection(fp)
        return self.__runstop_col

    @property
    def _descriptor_col(self):
        self._event_col
        if self.__descriptor_col is None:
            fp = os.path.join(self.config['directory'],
                              'event_descriptors.json')
            self.__descriptor_col = DescriptorCollection(self._event_col, fp)
        return self.__descriptor_col

    @property
    def _event_col(self):
        if self.__event_col is None:
            self.__event_col = EventCollection(self.config['directory'])
        return self.__event_col


class MDSRO(_CollectionMixin, MDSROTemplate):
    pass


class MDS(_CollectionMixin, MDSTemplate):
    pass
