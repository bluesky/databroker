import requests
from .utils import mds_setup, mds_teardown


def setup():
    mds_setup()

def test_smoke_urls():
    pass


def teardown():
    mds_teardown()
