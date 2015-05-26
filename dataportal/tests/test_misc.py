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

def test_pims_images():
    header = db[-1]
    images = Images(header, 'img')
    images[:5]  # smoke test
    assert_equal(images.pixel_type, np.float64)
    assert_array_equal(images.frame_shape, images[0].shape)
    assert_equal(len(images), image_and_scalar.num1)

def test_pims_subtracted_images():
    header = db[-1]
    images = Images(header, 'img')
    s_images = SubtractedImages(header, 'img', 'img')
    s_images[:5]  # smoke test
    assert_equal(s_images.pixel_type, np.float64)
    assert_array_equal(s_images.frame_shape, s_images[0].shape)
    assert_equal(len(s_images), image_and_scalar.num1)

    # We are subtracting an image from itself, so the result should be zero.
    expected = np.zeros(s_images.frame_shape)
    assert_array_equal(s_images[0], expected)

def setup():
    mds_setup()
    fs_setup()
    image_and_scalar.run()

def teardown():
    mds_teardown()
    fs_teardown()
