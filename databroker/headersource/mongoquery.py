from __future__ import absolute_import

import copy
import os
import json
from mongoquery import Query
from .base import MDSTemplate, MDSROTemplate
from .core import ASCENDING, DESCENDING
from ..utils import ensure_path_exists


class JSONCollection(object):
    def __init__(self, fp):
        self._fp = fp
        self.refresh()

    def refresh(self):
        if os.path.isfile(self._fp):
            with open(self._fp, 'r') as f:
                self._docs = json.load(f)
        else:
            self._docs = []
            with open(self._fp, 'w') as f:
                json.dump([], f)

    def find(self, query, sort=None):
        match = Query(query).match
        result = filter(match, self._docs)
        if sort is None:
            return (copy.deepcopy(elem) for elem in result)
        elif len(sort) > 2:
            raise NotImplementedError("Only one sort key is supported.")
        else:
            sort, = sort
            # ascending_or_descending is -1 (descending) or 1 (ascending)
            key, ascending_or_descending = sort
            reverse = (ascending_or_descending == DESCENDING)
            sorted_result = sorted(result, key=lambda x: x[key], reverse=reverse)
            # Make it a generator so it is the same as the unsorted code path.
            return (copy.deepcopy(elem) for elem in sorted_result)

    def find_one(self, query):
        match = Query(query).match
        for doc in self._docs:
            if match(doc):
                return copy.deepcopy(doc)
        return None

    def insert_one(self, doc, fk=None):
        self.refresh()
        if fk is not None:
            if self.find_one({fk: doc[fk]}) is not None:
                raise RuntimeError('Duplicate {}: {}'.format(fk, doc[fk]))
        self._docs.append(doc)
        with open(self._fp, 'w') as f:
            json.dump(self._docs, f)

    def insert(self, docs):
        self.refresh()
        self._docs.extend(docs)
        with open(self._fp, 'w') as f:
            json.dump(self._docs, f)


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
            self.__runstart_col = JSONCollection(fp)
        return self.__runstart_col

    @property
    def _runstop_col(self):
        if self.__runstop_col is None:
            fp = os.path.join(self.config['directory'], 'run_stops.json')
            self.__runstop_col = JSONCollection(fp)
        return self.__runstop_col

    @property
    def _descriptor_col(self):
        if self.__descriptor_col is None:
            fp = os.path.join(self.config['directory'],
                              'event_descriptors.json')
            self.__descriptor_col = JSONCollection(fp)
        return self.__descriptor_col

    @property
    def _event_col(self):
        if self.__event_col is None:
            fp = os.path.join(self.config['directory'], 'events.json')
            self.__event_col = JSONCollection(fp)
        return self.__event_col


class MDSRO(_CollectionMixin, MDSROTemplate):
    pass


class MDS(_CollectionMixin, MDSTemplate):
    pass
