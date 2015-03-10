import nose
from dataportal.replay.persist import History

h = None

def setup():
    h = History(':memory:')

def test_history():
    pass
