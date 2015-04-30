from ..utils.diagnostics import watermark
from ..broker.pims_readers import Images, SubtractedImages
from ..broker import DataBroker as db
from ..examples.sample_data import image_and_scalar
from metadatastore.utils.testing import mds_setup, mds_teardown
from filestore.utils.testing import fs_setup, fs_teardown
import numpy as np

from nose.tools import assert_equal
from numpy.testing.utils import assert_array_equal

def test_watermark():
    watermark()

def test_pims():
    header = db[-1]
    images = Images(header, 'img')
    images[:5]
    s_images = SubtractedImages(header, 'img', 'img')
    assert_equal(len(images), len(s_images))
    expected = np.zeros(s_images.frame_shape)
    assert_array_equal(s_images[0], expected)

def setup():
    mds_setup()
    fs_setup()
    image_and_scalar.run()

def teardown():
    mds_teardown()
    fs_teardown()
