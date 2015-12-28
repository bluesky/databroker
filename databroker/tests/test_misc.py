from databroker.utils.diagnostics import watermark
from databroker.pims_readers import Images, get_images
from databroker import DataBroker as db
from databroker.examples.sample_data import image_and_scalar
from metadataclient.testing_utils import mds_setup, mds_teardown
from filestore.utils.testing import fs_setup, fs_teardown
import numpy as np

from nose.tools import assert_equal
from numpy.testing.utils import assert_array_equal

def test_watermark():
    watermark()

def test_pims_images_old_api():
    header = db[-1]
    images = Images(header, 'img')
    images[:5]  # smoke test
    assert_equal(images.pixel_type, np.float64)
    assert_array_equal(images.frame_shape, images[0].shape)
    assert_equal(len(images), image_and_scalar.num1)


def test_pims_images():
    header = db[-1]
    images = get_images(header, 'img')
    images[:5]  # smoke test
    assert_equal(images.pixel_type, np.float64)
    assert_array_equal(images.frame_shape, images[0].shape)
    assert_equal(len(images), image_and_scalar.num1)


def setup():
    mds_setup()
    fs_setup()
    image_and_scalar.run()

def teardown():
    mds_teardown()
    fs_teardown()
