import uuid
from filestore.api import db_connect, db_disconnect


conn = None
db_name = str(uuid.uuid4())


def fs_setup():
    "Create a fresh database with unique (random) name."
    global conn
    db_disconnect()
    conn = db_connect(db_name, 'localhost', 27017)

def fs_teardown():
    "Drop the fresh database and disconnect."
    conn.drop_database(db_name)
    db_disconnect()
