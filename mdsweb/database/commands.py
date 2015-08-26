from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import motor
from tornado.gen import coroutine
#TODO: Implement dbschema classes




@coroutine
def insert_run_start():


def insert_run_stop():
    pass

def insert_beamline_config():
    pass

def insert_event_descriptor():
    pass

def insert_event():
    pass

def _bulk_event_write():
    pass

def _get_and_dump_header():
    pass

def _transpose_and_dump_event():
    pass

#TODO: Pass 'db' to tornado Application to make it available as request handler.
#TODO: Add a callback to run_stop that grabs all related documents and dumps them into the primary data store
#TODO: Authentication/authorization into mongo

