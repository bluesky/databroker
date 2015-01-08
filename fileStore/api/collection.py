__author__ = 'arkilic'

from fileStore.database.databaseInit import db, conn
from fileStore.database.mongoCollections import FileAttributes, FileBase, EventList
from pymongo.errors import OperationFailure, PyMongoError


def create_file_link(file_id, spec, file_path, custom=None):
    """
    :param file_id:
    :param spec:
    :param file_path:
    :param custom:
    :return:
    """
    #TODO: Validate file_id, file_path, and spec data types here
    if custom is None:
        custom = dict()
    f_base = FileBase(file_id=file_id, spec=spec, file_path=file_path, custom=custom)
    try:
        flink_id = db['file_base'].insert(f_base.bsonify(), wtimeout=100, write_concern={'w': 1})
    except OperationFailure:
        raise PyMongoError("File link entry insert failed due to database server connection")
    return flink_id


def create_file_attributes(shape, dtype, **kwargs):
    """

    :param shape:
    :param dtype:
    :param kwargs:
    :return:
    """
    f_attr = FileAttributes(shape=shape, dtype=dtype, **kwargs)
    try:
        f_attr_id = db['file_attributes'].insert(f_attr.bsonify(), wtimeout=100, write_concern={'w': 1})
    except OperationFailure:
        raise PyMongoError("File attributes entry insert failed due to database server connection")
    return f_attr_id

def create_event_list(event_id, file_id, event_list_custom=None):
    if event_list_custom is None:
        event_list_custom = dict()
    event_list = EventList(event_id=event_id, file_id=file_id, event_list_custom=event_list_custom)
    try:
        event_list_id = db['event_list'].insert(event_list.bsonify(), wtimeout=100, write_concern={'w': 1})
    except OperationFailure:
        raise PyMongoError("File event list entry failed due to database server connection")
    return event_list_id

