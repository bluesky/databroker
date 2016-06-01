import requests
from .utils import mds_setup, mds_teardown


def setup():
    mds_setup()


def teardown():
    mds_teardown()
