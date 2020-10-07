import xarray
from importlib import import_module

from .core import BlueskyRun


class ProjectionError(Exception):
    pass


def get_run_projection(run: BlueskyRun, projection_name: str = None):
    """Finds a projection in the run.
    If projection_name is provided, searches through the projections in the run
    to find a match.

    Otherwise, looks in the run to see if there is only one projection. If so, returns it.

    Parameters
    ----------
    run : BlueskyRun
        Run to investigate for a projection
    projection_name : str, optional
        name of the projection to look for, by default None

    Returns
    -------
    dict
        returns a projection dictionary, or None of not found

    Raises
    ------
    KeyError
        If the a projection_name is specified and there is more than one
        projection in the run with that name
    """

    if projection_name is not None:
        projections = [projection for projection in run.metadata['start']['projections']
                       if projection.get('name') == projection_name]
        if len(projections) > 1:
            raise KeyError("Multiple projections of name {projection_name} found")
        if len(projections) == 1:
            return projections[0]
        if len(projections) == 0:
            return None

    if 'projections' in run.metadata['start'] and len(run.metadata['start']['projections']) == 1:
        return run.metadata['start']['projections'][0]

    return None


def get_calculated_value(run: BlueskyRun, key: str, mapping: dict):
    """Calls and returns the callable from the calculated projection mapping.

    It is ancticipated that the return will be
    and xarray.DataArray.

    This should be expressed in the familiar 'module:func' syntax borrowed from python entry-points.

    An example implementation of a calculated field projection entry:

        '/projection/key': {
            "type": "calculated",
            "callable": "foo.bar:really_fun",
            "args": ['arg1'], "kwargs": {"foo": "bar"}}

    And a corresponding function implementation might be:

        def really_fun(run, *args, **kwargs)"
            # args will be ['arg1']
            # kwargs will be {"foo": "bar"}
            # for this calculated field
            return xarray.DataArray[[1, 2, 3]]


    Parameters
    ----------
    run : BlueskyRun
        run which can be used for the calcuation
    key : str
        key name for this projection
    mapping : dict
        full contents of this projection

    Returns
    -------
    any
        result of calling the method specified in the calcated field in the projection

    Raises
    ------
    ProjectionError
        [description]
    """
    callable_name = mapping['callable']
    try:
        module_name, function_name = callable_name.split(":")
        module = import_module(module_name)
        callable_func = getattr(module, function_name)
    except ProjectionError as e:
        raise ProjectionError('Error importing callable {function_name}', e)

    calc_args = mapping['args']
    calc_kwargs = mapping['kwargs']
    return callable_func(run, *calc_args, **calc_kwargs)


class Projector():

    def __init__(self, metadata_callback=None, event_configuration_callback=None, event_field_callback=None):
        self._metadata_callback = metadata_callback
        self._event_configuration_callback = event_configuration_callback
        self._event_field_callback = event_field_callback

    def project(self, run: BlueskyRun, projection=None, projection_name=None):
        try:
            if projection is None:
                projection = get_run_projection(run, projection_name)
            if projection is None:
                raise ProjectionError("Projection could not be found")

            # attrs = {}  # will populate the return Dataset attrs field
            # data_vars = {}  # will populate the return Dataset DataArrays
            for field_key, mapping in projection['projection'].items():
                # go through each projection
                projection_type = mapping['type']
                projection_location = mapping.get('location')
                projection_data = None
                projection_linked_field = mapping.get('field')
                # single value data that will go in the top
                # dataset's attributes
                if projection_location == 'start':
                    self._metadata_callback(field_key, run.metadata['start'][projection_linked_field])
                    continue

                # added to return Dataset in data_vars dict
                if projection_type == "calculated":
                    self._event_field_callback(field_key, get_calculated_value(run, field_key, mapping))
                    continue

                # added to return Dataset in data_vars dict
                if projection_location == 'event':
                    projection_stream = mapping.get('stream')
                    if projection_stream is None:
                        raise ProjectionError(f'stream missing for event projection: {field_key}')
                    try:
                        # TODO check if field exists in stream first
                        self._event_field_callback(field_key, run[projection_stream]
                                                   .to_dask()[projection_linked_field])
                    except Exception as e:
                        raise ProjectionError(f'error projecting field: {field_key}') from e

                elif projection_location == 'configuration':
                    self._event_configuration_callback(field_key, projection_data)
                else:
                    raise KeyError(f'Unknown location: {projection_location} in projection.')

        except ProjectionError as e:
            raise e
        except Exception as e:
            raise ProjectionError('Error projecting run') from e


def project_xarray(run: BlueskyRun, *args, projection=None, projection_name=None, **kwargs):
    """Produces an xarray Dataset by projecting the provided run. Selects projection based on
    logic of get_run_projection().


    Projections come with multiple types: linked, and caclulated. Calculated fields are only supported
    in the data (not at the top-level attrs).

    Calculated fields in projections schema contain a callable field. This should be expressed in
    the familiar 'module:func' syntax borrowed from python entry-points.

    All projection fields with "location"=="start" will look in the run_start
    for metadata. Each field will be added to the return Dataset's attrs dictionary keyed
    on projection key.

    All projection fields with "location"=="configuration" will look in the event_descriptor.configuration field
    for settings that appear once per stream. Each field will be added to the return Dataset's attrs dictionary keyed
    on projection key.

    All projection fields with "location"=="event" will look for a field in a stream.

    Parameters
    ----------
    run : BlueskyRun
        run to project
    projection_name : str, optional
        name of a projection to select in the run, by default None
    projection : dict, optional
        projection not from the run to use, by default None

    Returns
    -------
    xarray.Dataset
         The return Dataset will contain:
        - single value meta data (from the run start) in the return Dataset's attrs dict, keyed
        on the projection key. These are projections marked  "location": "start"

        The return xarray.Dataset will contain 

        - single value meata data (from a streams configuration field) in the return Dataset's xarray's dict, keyed
        on the projection key. These are projections marked  "location": "start"

        - multi-value data (from a stream). Keys for the dict-like xarray.Dataset match keys
        in the passed-in projection. These are projections with "location": "linked"

    Raises
    ------
    ProjectionError
    """
    try:

        attrs = {}  # will populate the return Dataset attrs field
        data_vars = {}  # will populate the return Dataset DataArrays

        def metadata_callback(field, value):
            attrs[field] = value

        def event_configuration_callback(field, value):
            ...

        def event_field_callback(field, value):
            data_vars[field] = value

        projector = Projector(
            metadata_callback=metadata_callback, 
            event_configuration_callback=event_configuration_callback,
            event_field_callback=event_field_callback)

        projector.project(run, projection=projection, projection_name=projection_name)

    except ProjectionError as e:
        raise e
    except Exception as e:
        raise ProjectionError('Error projecting run') from e
    return xarray.Dataset(data_vars, attrs=attrs)
