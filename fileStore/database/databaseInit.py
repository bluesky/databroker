__author__ = 'arkilic'

from .. import EID_KEY, FID_KEY

from pymongo import MongoClient

# TODO: Add native python logger and pyOlog hook
from fileStore.database.mongoCollections import (FileAttributes, FileBase,
                                                 EventList)


class FSDB(object):
    def __init__(self, host, port, database):
        self.conn = MongoClient(host=host, port=int(port))
        self.db = self.conn[database]
        self.db['event_list'].ensure_index(EID_KEY, unique=True)
        self.db['file_base'].ensure_index(FID_KEY, unique=True)

    _connection_err_msg = ("{} entry insert failde due to " +
                           "database server connection")

    def create_file_link(self, file_id, spec, file_path, custom=None):
        """
        :param file_id:
        :param spec:
        :param file_path:
        :param custom:
        :return:
        """
        # TODO: Validate file_id, file_path, and spec data types here
        if custom is None:
            custom = dict()
        f_base = FileBase(file_id=file_id, spec=spec,
                          file_path=file_path, custom=custom)
        flink_id = self.db['file_base'].insert(f_base.bsonify(),
                                                   wtimeout=100,
                                                   write_concern={'w': 1})

        return flink_id

    def create_file_attributes(self, shape, dtype, **kwargs):
        """

        :param shape:
        :param dtype:
        :param kwargs:
        :return:
        """
        f_attr = FileAttributes(shape=shape, dtype=dtype, **kwargs)
        f_attr_id = self.db['file_attributes'].insert(
            f_attr.bsonify(), wtimeout=100, write_concern={'w': 1})

        return f_attr_id

    def create_event_list(self, event_id, file_id, event_list_custom=None):
        if event_list_custom is None:
            event_list_custom = dict()

        event_list = EventList(event_id=event_id, file_id=file_id,
                               event_list_custom=event_list_custom)
        event_list_id = self.db['event_list'].insert(
                event_list.bsonify(), wtimeout=100, write_concern={'w': 1})

        return event_list_id
