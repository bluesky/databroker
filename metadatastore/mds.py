from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import pymongo
from pymongo import MongoClient
import boltons.cacheutils
from . import core

class _DBManager(object):
    def __init__(self, config):
        self.config = config

        self.__conn = None

        self.__db = None

        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None

    def disconnect(self):

        self.__conn = None

        self.__db = None

        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None

    def reconfigure(self, config):
        self.disconnect()
        self.config = config

    @property
    def _connection(self):
        if self.__conn is None:
            self.__conn = MongoClient(self.config['host'],
                                      self.config.get('port', None))
        return self.__conn

    @property
    def _db(self):
        if self.__db is None:
            conn = self._connection
            self.__db = conn.get_database(self.config['database'])
        return self.__db

    @property
    def _runstart_col(self):
        if self.__runstart_col is None:
            self.__runstart_col = self._db.get_collection('run_start')

            self.__runstart_col.create_index([('uid', pymongo.DESCENDING)],
                                             unique=True)
            self.__runstart_col.create_index([('time', pymongo.DESCENDING),
                                              ('scan_id', pymongo.DESCENDING)],
                                             unique=False, background=True)

        return self.__runstart_col

    @property
    def _runstop_col(self):
        if self.__runstop_col is None:
            self.__runstop_col = self._db.get_collection('run_stop')

            self.__runstop_col.create_index([('run_start', pymongo.DESCENDING),
                                            ('uid', pymongo.DESCENDING)],
                                            unique=True)
            self.__runstop_col.create_index([('time', pymongo.DESCENDING)],
                                            unique=False, background=True)

        return self.__runstop_col

    @property
    def _descriptor_col(self):
        if self.__descriptor_col is None:
            self.__descriptor_col = self._db.get_collection('event_descriptor')

            self.__descriptor_col.create_index([('uid', pymongo.DESCENDING)],
                                               unique=True)
            self.__descriptor_col.create_index(
                [('run_start', pymongo.DESCENDING),
                 ('time', pymongo.DESCENDING)],
                unique=False, background=True)

        return self.__descriptor_col

    @property
    def _event_col(self):
        if self.__event_col is None:
            self.__event_col = self._db.get_collection('event')

            self.__event_col.create_index([('uid', pymongo.DESCENDING)],
                                          unique=True)
            self.__event_col.create_index([('descriptor', pymongo.DESCENDING),
                                           ('time', pymongo.DESCENDING)],
                                          unique=False, background=True)

        return self.__event_col

    @property
    def _datum_col(self):
        if self.__datum_col is None:
            self.__datum_col = self._db.get_collection('datum')
            self.__datum_col.create_index('datum_id', unique=True)

        return self.__datum_col


class MDSRO(object):
    def __init__(self, config):
        self._DB = _DBManager(config)
        self._RUNSTART_CACHE = boltons.cacheutils.LRU(max_size=1000)
        self._RUNSTOP_CACHE = boltons.cacheutils.LRU(max_size=1000)
        self._DESCRIPTOR_CACHE = boltons.cacheutils.LRU(max_size=1000)

    def clear_process_cache(self):
        """Clear all local caches"""
        self._RUNSTART_CACHE.clear()
        self._RUNSTOP_CACHE.clear()
        self._DESCRIPTOR_CACHE.clear()

    def db_disconnect(self):
        """Helper function to deal with stateful connections to mongoengine"""
        self._DB.disconnect()
        self.clear_process_cache()

    def db_connect(self, database, host, port):
        """Helper function to deal with stateful connections to mongoengine

        .. warning

           This will silently ignore input if the database is already
           connected, even if the input database, host, or port are
           different than currently connected.  To change the database
           connection you must call `db_disconnect` before attempting to
           re-connect.
        """
        self.clear_process_cache()
        self._DB.reconfigure(dict(database=database,
                                            host=host, port=port))
        return self._DB._connection

    def run_start_given_uid(self, uid):
        """Given a uid, return the RunStart document

        Parameters
        ----------
        uid : str
            The uid

        Returns
        -------
        run_start : doc.Document
            The RunStart document.

        """
        return core.run_start_given_uid(uid, self._DB._runstop_col,
                                        self._RUNSTOP_CACHE)

    def descriptor_given_uid(self, uid):
        """Given a uid, return the EventDescriptor document

        Parameters
        ----------
        uid : str
            The uid

        Returns
        -------
        descriptor : doc.Document
            The EventDescriptor document fully de-referenced
        """
        return core.descriptor_given_uid(uid, self._DB._descriptor_col,
                                         self._DESCRIPTOR_CACHE)
