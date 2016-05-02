import pickle

from metadatastore.mds import MDS


def test_pickle():
    md = MDS(config={'host': 'portland'}, version=1)
    md2 = pickle.loads(pickle.dumps(md))

    assert md.version == md2.version
    assert md.config == md2.config
