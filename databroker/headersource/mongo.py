from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import pymongo
from pymongo import MongoClient
from . import mongo_core
from .base import MDSROTemplate, MDSTemplate


class MDSRO(MDSROTemplate):
    _API_MAP = {1: mongo_core}

    def __init__(self, config, auth=False):
        super(MDSRO, self).__init__(config)
        self.reset_connection()
        self.auth = auth

    def reset_connection(self):
        self.__conn = None

        self.__db = None

        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None

    def __setstate__(self, state):
        # TODO likely broken with auth?
        self._RUNSTART_CACHE = {}
        self._RUNSTOP_CACHE = {}
        self._DESCRIPTOR_CACHE = {}
        self.reset_connection()
        self._api = None
        self.version, self.config = state

    def disconnect(self):
        self.__conn = None
        self.__db = None
        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None

    def reconfigure(self, config):
        self.disconnect()
        super(MDSRO, self).reconfigure(config)

    @property
    def _connection(self):
        if self.__conn is None:
            if self.auth:
                uri = 'mongodb://{0}:{1}@{2}:{3}/'.format(
                    self.config['mongo_user'],
                    self.config['mongo_pwd'],
                    self.config['host'],
                    self.config['port'])
                self.__conn = MongoClient(uri)
            else:
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
            self.__runstart_col.create_index([("$**", "text")])

        return self.__runstart_col

    @property
    def _runstop_col(self):
        if self.__runstop_col is None:
            self.__runstop_col = self._db.get_collection('run_stop')
            self.__runstop_col.create_index('run_start',
                                            unique=True)
            self.__runstop_col.create_index('uid',
                                            unique=True)
            self.__runstop_col.create_index([('time', pymongo.DESCENDING)],
                                            unique=False, background=True)
            self.__runstop_col.create_index([("$**", "text")])

        return self.__runstop_col

    @property
    def _descriptor_col(self):
        if self.__descriptor_col is None:
            # The name of the reference to the run start changed from
            # 'run_start_id' in v0 to 'run_start' in v1.
            rs_name = 'run_start'

            self.__descriptor_col = self._db.get_collection('event_descriptor')

            self.__descriptor_col.create_index([('uid', pymongo.DESCENDING)],
                                               unique=True)
            self.__descriptor_col.create_index([(rs_name, pymongo.DESCENDING),
                                                ('time', pymongo.DESCENDING)],
                                               unique=False, background=True)
            self.__descriptor_col.create_index([('time', pymongo.DESCENDING)],
                                               unique=False, background=True)
            self.__descriptor_col.create_index([("$**", "text")])

        return self.__descriptor_col

    @property
    def _event_col(self):
        if self.__event_col is None:
            self.__event_col = self._db.get_collection('event')

            self.__event_col.create_index([('uid', pymongo.DESCENDING)],
                                          unique=True)
            self.__event_col.create_index([('descriptor', pymongo.DESCENDING),
                                           ('time', pymongo.ASCENDING)],
                                          unique=False, background=True)
        return self.__event_col

    def db_disconnect(self):
        """Helper function to deal with stateful connections to mongoengine"""
        self.disconnect()
        self.clear_process_cache()

    def db_connect(self, database, host, port, **kwargs):
        """Helper function to deal with stateful connections to mongoengine

        .. warning

           This will silently ignore input if the database is already
           connected, even if the input database, host, or port are
           different than currently connected.  To change the database
           connection you must call `db_disconnect` before attempting to
           re-connect.
        """
        self.clear_process_cache()
        self.reconfigure(dict(database=database,
                              host=host, port=port, **kwargs))
        return self._connection

    def find_events(self, **kwargs):
        """Given search criteria, locate Event Documents.

        Parameters
        -----------
        since : time-like, optional
            time-like representation of the earliest time that an Event
            was created. Valid options are:
               - timestamps --> time.time()
               - '2015'
               - '2015-01'
               - '2015-01-30'
               - '2015-03-30 03:00:00'
               - datetime.datetime.now()
        until : time-like, optional
            timestamp of the latest time that an Event was created. See
            docs for `since` for examples.
        descriptor : doc.Document or str, optional
           Find events for a given EventDescriptor
        uid : str, optional
            Globally unique id string provided to metadatastore

        Returns
        -------
        events : iterable of doc.Document objects
        """
        gen = self._api.find_events(self._runstart_col,
                                    self._RUNSTART_CACHE,
                                    self._descriptor_col,
                                    self._DESCRIPTOR_CACHE,
                                    self._event_col,
                                    self.config['timezone'],
                                    **kwargs)
        for ev in gen:
            yield ev


class MDS(MDSRO, MDSTemplate):
    _INS_METHODS = {'start': 'insert_run_start',
                    'stop': 'insert_run_stop',
                    'descriptor': 'insert_descriptor',
                    'event': 'insert_event',
                    'bulk_events': 'bulk_insert_events'}
