__author__ = 'arkilic'

from fileStore.conf import database, host, port
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

#TODO: Add native python logger and pyOlog hook

try:
    conn = MongoClient(host=host, port=int(port))
    db = conn[database]
except ConnectionFailure:
    raise ConnectionFailure("Connection to Mongo server cannot be established. Make sure Mongo Daemon is running."
                            "Please check your host, port, and database configuration")

