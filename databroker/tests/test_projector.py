import pytest
import xarray

from ..projector import XarrayProjector, ProjectionError

NEX_IMAGE_FIELD = '/entry/instrument/detector/data'
NEX_ENERGY_FIELD = '/entry/instrument/monochromator/energy'
NEX_SAMPLE_NAME_FIELD = '/entry/sample/name'

good_projection = [{
        "name": "nxsans",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            NEX_SAMPLE_NAME_FIELD: {"location": "configuration", "field": "sample"},
            NEX_IMAGE_FIELD: {"location": "event", "stream": "primary", "field": "ccd"},
            NEX_ENERGY_FIELD: {"location": "event", "stream": "primary", "field": "beamline_energy"}

        }
    }]

bad_location = [{
        "name": "nxsans",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            NEX_SAMPLE_NAME_FIELD: {"location": "i_dont_exist", "field": "sample"},

        }
    }]

bad_stream = [{
        "name": "nxsans",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            NEX_SAMPLE_NAME_FIELD: {"location": "configuration", "field": "sample"},
            NEX_IMAGE_FIELD: {"location": "event", "stream": "i_dont_exist", "field": "ccd"},

        }
    }]

bad_field = [{
        "name": "nxsans",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            NEX_SAMPLE_NAME_FIELD: {"location": "configuration", "field": "sample"},
            NEX_IMAGE_FIELD: {"location": "event", "stream": "primary", "field": "i_dont_exist"},

        }
    }]


def test_unknown_location():
    mock_run = make_mock_run(bad_location, 'one_ring')
    with pytest.raises(ProjectionError):
        projector = XarrayProjector(mock_run)
        projector.project(bad_location[0])


def test_nonexistent_stream():
    mock_run = make_mock_run(bad_stream, 'one_ring')
    with pytest.raises(ProjectionError):
        projector = XarrayProjector(mock_run)
        projector.project(bad_stream[0])


def test_projector():
    mock_run = make_mock_run(good_projection, 'one_ring')
    projector = XarrayProjector(mock_run)
    dataset = projector.project(good_projection[0])
    # Ensure that the to_dask function was called on both 
    # energy and image datasets
    assert mock_run['primary'].to_dask_counter == 2
    assert dataset.attrs[NEX_SAMPLE_NAME_FIELD]
    for idx, energy in enumerate(dataset[NEX_ENERGY_FIELD]):
        assert energy == mock_run['primary'].dataset['beamline_energy'][idx]
    for idx, image in enumerate(dataset[NEX_IMAGE_FIELD]):
        comparison = image == mock_run['primary'].dataset['ccd'][idx]
        assert comparison.all()


MOCK_IMAGE = xarray.DataArray([[1, 2], [3, 4]])
BEAMLINE_ENERGY_VALS = [1, 2, 3, 4, 5]
I_ZERO_VALS = [-1, -2, -3, -4, -5]
CCD = [MOCK_IMAGE+1, MOCK_IMAGE+2, MOCK_IMAGE+3, MOCK_IMAGE+4, MOCK_IMAGE+5]


class MockStream():
    def __init__(self, metadata):
        self.metadata = metadata
        data_vars = {
            'beamline_energy': ('time', BEAMLINE_ENERGY_VALS),
            'ccd': (('time', 'dim_0', 'dim_1'), CCD)
        }
        self.dataset = xarray.Dataset(data_vars)
        self.to_dask_counter = 0

    def to_dask(self):
        # This enables us to test that the to_dask function is called
        # the appropriate number of times.
        # It would be better if we could actually return the dataset as a dask dataframe
        # However, for some reason this won't let us access the arrays
        #  by numeric index and will throw an error
        self.to_dask_counter += 1
        return self.dataset


class MockRun():
    def __init__(self, projections=[], sample='',):
        self.metadata = {
            'start': {
                'sample': sample,
                'projections': projections
            },
            'stop': {}
        }

        self.primary = MockStream(self.metadata)

    def __getitem__(self, key):
        if key == 'primary':
            return self.primary
        raise KeyError(f'Key: {key}, does not exist')


def make_mock_run(projections, sample):
    return MockRun(projections, sample)
