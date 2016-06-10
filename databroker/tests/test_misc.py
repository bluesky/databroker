import time as ttime
import uuid

import numpy as np
import pytest
from filestore.test.utils import fs_setup, fs_teardown
from metadatastore.commands import insert_run_start
from metadatastore.test.utils import mds_setup, mds_teardown
from numpy.testing.utils import assert_array_equal

from databroker import DataBroker as db
from databroker.pims_readers import Images, get_images
from ..examples.sample_data import image_and_scalar
from ..utils.diagnostics import watermark


@pytest.fixture(scope='module')
def image_uid():
    rs = insert_run_start(time=ttime.time(), scan_id=105,
                          owner='stepper', beamline_id='example',
                          uid=str(uuid.uuid4()), cat='meow')
    image_and_scalar.run(run_start_uid=rs)
    return rs


def setup_module(module):
    mds_setup()
    fs_setup()


def teardown_module(module):
    mds_teardown()
    fs_teardown()


def test_watermark():
    result = watermark()
    assert result


def test_pims_images_old_api(image_uid):
    header = db[image_uid]
    images = Images(header, 'img')
    images[:5]  # smoke test
    assert images.pixel_type == np.float64
    assert_array_equal(images.frame_shape, images[0].shape)
    assert len(images) == image_and_scalar.num1


def test_pims_images(image_uid):
    header = db[image_uid]
    images = get_images(header, 'img')
    images[:5]  # smoke test
    assert images.pixel_type == np.float64
    assert_array_equal(images.frame_shape, images[0].shape)
    assert len(images) == image_and_scalar.num1

