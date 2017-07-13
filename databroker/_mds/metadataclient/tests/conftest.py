import pytest
from ....headersource import client as mds
from ...metadataservice.test import utils
variations = [mds]


@pytest.fixture(params=variations, scope='module')
def mds_all(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.
    '''

    utils.mds_setup()
    tmds = request.param.MDS({'host': 'localhost',
                              'port': 9009,
                              'timezone': 'US/Eastern'})

    request.addfinalizer(utils.mds_teardown)

    return tmds
