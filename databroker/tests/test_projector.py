import pytest

import xarray

from ..projector import (
    get_run_projection,
    project_xarray,
    get_xarray_config_field,
    project_summary_dict
)

EVENT_FIELD = 'event_field_name'
EVENT_CONFIGURATION_FIELD = 'event_configuration_name'
START_DOC_FIELD = 'start_doc_metadata_name'
START_DOC_FIELD_2 = 'start_doc_metadata_name_2'
MOCK_IMAGE = xarray.DataArray([[1, 2], [3, 4]])
BEAMLINE_ENERGY_VALS = [1, 2, 3, 4, 5]
OTHER_VALS = [-1, -2, -3, -4, -5]
CCD = [MOCK_IMAGE+1, MOCK_IMAGE+2, MOCK_IMAGE+3, MOCK_IMAGE+4, MOCK_IMAGE+5]

good_projection = [{
        "name": "nxsas",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            START_DOC_FIELD: {"type": "linked", "location": "start", "field": "sample"},
            START_DOC_FIELD_2: {"type": "linked", "location": "start", "field": "sample"},
            EVENT_FIELD: {"type": "linked", "location": "event", "stream": "primary", "field": "ccd"},
            EVENT_CONFIGURATION_FIELD: {"type": "linked",
                                        "location": "configuration",
                                        "stream": "primary",
                                        "config_index": 0,
                                        "config_device": "camera_thingy",
                                        "field": "camera_manufacturer"},
        }
    }]

bad_location = [{
        "name": "nxsas",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            START_DOC_FIELD: {"type": "linked", "location": "i_dont_exist", "field": "sample"},

        }
    }]

bad_stream = [{
        "name": "nxsas",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            START_DOC_FIELD: {"type": "linked", "location": "start", "field": "sample"},
            EVENT_FIELD: {"type": "linked", "location": "event", "stream": "i_dont_exist", "field": "ccd"},

        }
    }]

bad_field = [{
        "name": "nxsas",
        "version": "2020.1",
        "configuration": {"name": "RSoXS"},
        "projection": {
            START_DOC_FIELD: {"type": "linked", "location": "start", "field": "sample"},
            EVENT_FIELD: {"type": "linked", "location": "event", "stream": "primary", "field": "i_dont_exist"},

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
                'uid': 42,
                'sample': sample,
                'projections': projections
            },
            'descriptors': [
                {
                    'configuration': {
                        'camera_thingy': {
                            'data': {'camera_manufacturer': 'berkeley lab'}
                        }
                    }

                }
            ],
            'stop': {}
        }
        self.primary = MockStream(self.metadata)

    def __getitem__(self, key):
        if key == 'primary':
            return self.primary
        raise KeyError(f'Key: {key}, does not exist')


def make_mock_run(projections, sample):
    return MockRun(projections, sample)


def dont_panic(run, *args, **kwargs):
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
                "location": "event",
                "stream": "stream_name",
                "field": "field_name",
                "callable": "databroker.tests.test_projector:dont_panic",
                "args": ['trillian'], "kwargs": {"ford": "prefect"}}
        }
    }]

    mock_run = make_mock_run(calculated_projection, 'garggle_blaster')
    dataset, issues = project_xarray(mock_run)
    assert len(issues) == 0
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
    dataset, issues = project_xarray(mock_run)
    assert len(issues) > 0


def test_nonexistent_stream():
    mock_run = make_mock_run(bad_stream, 'one_ring')
    dataset, issues = project_xarray(mock_run)
    assert len(issues) > 0


def test_xarray_projector():
    mock_run = make_mock_run(good_projection, 'one_ring')
    dataset, issues = project_xarray(mock_run)
    # Ensure that the to_dask function was called on both
    # energy and image datasets
    assert mock_run['primary'].to_dask_counter == 1
    assert get_xarray_config_field(dataset, EVENT_FIELD, 0, 'camera_thingy', 'camera_manufacturer') == \
        'berkeley lab'
    for idx, image in enumerate(dataset[EVENT_FIELD]):
        comparison = image == mock_run['primary'].dataset['ccd'][idx]  # xarray of comparison results
        assert comparison.all()  # False if comparision does not contain all True


def test_summary_projector():
    mock_run = make_mock_run(good_projection, 'one_ring')
    dataset, issues = project_summary_dict(mock_run)
    assert len(issues) == 0
    projection_fields = []
    for field, value in good_projection[0]['projection'].items():
        if 'location' in value and value['location'] == 'start':
            projection_fields.append(field)

    assert len(dataset) > 0
    for field in projection_fields:
        assert dataset[START_DOC_FIELD] == 'one_ring'
        assert dataset[START_DOC_FIELD_2] == 'one_ring'


def test_summary_projector_filtered():
    mock_run = make_mock_run(good_projection, 'one_ring')
    dataset, issues = project_summary_dict(mock_run, return_fields=[START_DOC_FIELD_2])
    assert len(issues) == 0
    assert len(dataset) == 1
    assert dataset[START_DOC_FIELD_2] == 'one_ring'
