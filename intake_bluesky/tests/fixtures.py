from bluesky import RunEngine
from bluesky.plans import scan
from bluesky.preprocessors import SupplementalData
import event_model
import ophyd.sim
import pytest


@pytest.fixture
def hw():
    return ophyd.sim.hw()  # a SimpleNamespace of simulated devices


SIM_DETECTORS = {'scalar': 'det',
                 'image': 'direct_img',
                 'external_image': 'img'}


@pytest.fixture(params=['scalar', 'image', 'external_image'])
def detector(request, hw):
    return getattr(hw, SIM_DETECTORS[request.param])


@pytest.fixture
def example_data(hw, detector):
    RE = RunEngine({})
    sd = SupplementalData(baseline=[hw.motor])
    RE.preprocessors.append(sd)

    docs = []

    def collect(name, doc):
        docs.append((name, event_model.sanitize_doc(doc)))

    uid, = RE(scan([detector], hw.motor, -1, 1, 20), collect)
    return uid, docs
