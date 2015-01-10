__author__ = 'arkilic'

from fileStore.database.databaseInit import FSDB
from fileStore.conf import database, host, port


__all__ = ['create_file_link', 'create_file_attributes', 'create_event_list']

db_obj = FSDB(host, port, database)

create_event_list = db_obj.create_event_list
create_file_attributes = db_obj.create_file_attributes
create_file_link = db_obj.create_file_link
