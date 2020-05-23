import pytest


def test_glue(db, RE):
    pytest.importorskip('glue')
    from databroker.glue import read_header
    from glue.qglue import parse_data
    import bluesky.plans as bp
    from ophyd.sim import SynAxis, SynGauss
    motor = SynAxis(name='motor')
    det = SynGauss('det', motor, 'motor', center=0, Imax=1,
                   noise='uniform', sigma=1, noise_multiplier=0.1)
    RE.subscribe(db.insert)
    RE(bp.scan([det], motor, -5, 5, 10))

    d = read_header(db[-1])
    g = parse_data(d[0], 'test')[0].to_dataframe()
