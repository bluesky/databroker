import pytest
import xarray
from databroker.core import BlueskyRun

from ..projector import get_run_projection, project_xarray, ProjectionError

NEX_IMAGE_FIELD = '/entry/instrument/detector/data'
NEX_ENERGY_FIELD = '/entry/instrument/monochromator/energy'
NEX_SAMPLE_NAME_FIELD = '/entry/sample/name'
MOCK_IMAGE = xarray.DataArray([[1, 2], [3, 4]])
BEAMLINE_ENERGY_VALS = [1, 2, 3, 4, 5]
OTHER_VALS = [-1, -2, -3, -4, -5]
CCD = [MOCK_IMAGE+1, MOCK_IMAGE+2, MOCK_IMAGE+3, MOCK_IMAGE+4, MOCK_IMAGE+5]
good_projection = [{
        "name": "nxsas",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            NEX_SAMPLE_NAME_FIELD: {"type": "linked", "location": "configuration", "field": "sample"},
            NEX_IMAGE_FIELD: {"type": "linked", "location": "event", "stream": "primary", "field": "ccd"},
            NEX_ENERGY_FIELD: {"type": "linked", "location": "event", "stream": "primary",
                               "field": "beamline_energy"},
        }
    }]

bad_location = [{
        "name": "nxsas",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            NEX_SAMPLE_NAME_FIELD: {"type": "linked", "location": "i_dont_exist", "field": "sample"},

        }
    }]

bad_stream = [{
        "name": "nxsas",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            NEX_SAMPLE_NAME_FIELD: {"type": "linked", "location": "configuration", "field": "sample"},
            NEX_IMAGE_FIELD: {"type": "linked", "location": "event", "stream": "i_dont_exist", "field": "ccd"},

        }
    }]

bad_field = [{
        "name": "nxsas",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            NEX_SAMPLE_NAME_FIELD: {"type": "linked", "location": "configuration", "field": "sample"},
            NEX_IMAGE_FIELD: {"type": "linked", "location": "event", "stream": "primary", "field": "i_dont_exist"},

        }
    }]

projections_same_name = [
        {
         "name": "nxsas"
        },
        {
         "name": "nxsas"
        }
    ]


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


def dont_panic(run: BlueskyRun, *args, **kwargs):
    # TODO test that args and kwargs are passed
    return xarray.DataArray([42, 42, 42, 42, 42])


def test_calculated_projections():
    calculated_projection = [{
        "name": "nxsas",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            '/entry/event/computed': {
                "type": "calculated",
                "callable": "databroker.tests.test_projector:dont_panic",
                "args": ['trillian'], "kwargs": {"ford": "prefect"}}
        }
    }]

    mock_run = make_mock_run(calculated_projection, 'garggle_blaster')
    dataset = project_xarray(mock_run)
    comparison = dataset['/entry/event/computed'] == [42, 42, 42, 42, 42]
    assert comparison.all()


def test_find_projection_in_run():
    mock_run = make_mock_run(good_projection, 'one_ring')
    assert get_run_projection(mock_run, projection_name="nxsas") == good_projection[0]
    assert get_run_projection(mock_run, projection_name="vogons") is None
    assert get_run_projection(mock_run) == good_projection[0]  # only one projection in run so choose it
    with pytest.raises(KeyError):
        mock_run = make_mock_run(projections_same_name, 'one_ring')
        get_run_projection(mock_run, projection_name="nxsas")


def test_unknown_location():
    mock_run = make_mock_run(bad_location, 'one_ring')
    with pytest.raises(ProjectionError):
        projector = project_xarray(mock_run)
        projector.project(bad_location[0])


def test_nonexistent_stream():
    mock_run = make_mock_run(bad_stream, 'one_ring')
    with pytest.raises(ProjectionError):
        projector = project_xarray(mock_run)
        projector.project(bad_stream[0])


def test_projector():
    mock_run = make_mock_run(good_projection, 'one_ring')
    dataset = project_xarray(mock_run)
    # Ensure that the to_dask function was called on both
    # energy and image datasets
    assert mock_run['primary'].to_dask_counter == 2
    assert dataset.attrs[NEX_SAMPLE_NAME_FIELD] == mock_run.metadata['start']['sample']
    for idx, energy in enumerate(dataset[NEX_ENERGY_FIELD]):
        assert energy == mock_run['primary'].dataset['beamline_energy'][idx]
    for idx, image in enumerate(dataset[NEX_IMAGE_FIELD]):
        comparison = image == mock_run['primary'].dataset['ccd'][idx]  # xarray of comparison results
        assert comparison.all()  # False if comparision does not contain all True
