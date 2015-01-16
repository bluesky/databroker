import uuid
import mongoengine
import mongoengine.connection
from nose.tools import make_decorator
from mongoengine.context_managers import switch_db

import fileStore.commands as fc
from fileStore.database.file_base import FileBase
from fileStore.database.file_event_link import FileEventLink

db_name = str(uuid.uuid4())
# need to make 'default' connection to point to no-where, just to be safe
conn = mongoengine.connect(str(uuid.uuid4()))


def setup():
    mongoengine.connect(db_name, alias='test_db')


def teardown():
    conn = mongoengine.connection.get_connection('test_db')
    conn.drop_database(db_name)


def context_decorator(func):
    def inner():
        with switch_db(FileBase, 'test_db'), \
          switch_db(FileEventLink, 'test_db'):
            func()

    return make_decorator(func)(inner)


@context_decorator
def test_smoke():
    for j in range(10):

        fb = fc.save_file_base('syn-mod', '',
                               {'shape': (50, 70)})
        for k in range(10):
            fc.save_file_event_link(fb,
                                    '{}_{}'.format(j, k), {'n': k})
