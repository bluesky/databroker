import xarray
from importlib import import_module

from .core import BlueskyRun


class ProjectionError(Exception):
    pass


class Projector():
    """Projector super class. This class should not be used directly. Rather
    it is anticapted that subclasses will serve the purpose of deciding 
    how to map a run to a datastructure using a projection.
    """
    def __init__(self):
        super().__init__(self)

    def __call__(self, run: BlueskyRun, *args, **kwargs):
        """Project the given run using the projection dictionary provided. 
        Projections must comply with the Projection spec from event_model.


        Parameters
        ----------
        projection : dict
            [description]
        """
        raise NotImplementedError

    @staticmethod
    def get_run_projection(run: BlueskyRun, projection_name: str = None):
        """Finds a projection, either from the run.
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


class XarrayProjector(Projector):
    """ Subclass of a projector that produces xarrays.
    """
    def __init__(self):
        pass

    def __call__(self, run: BlueskyRun, *args, projection=None, projection_name=None, **kwargs):
        """Produces an xarray Dataset by projecting the provided run. Selects projection based on
        logic of Projector.get_run_projection(). 
        The return Dataset will contain:
            - single value data (typically from the run start) in the return Dataset's attrs dict, keyed 
            on the projection key. These are projections marked  "location": "configuration" 

            - multi-value data (typically from a stream). Keys for the dict-like xarray.Dataset match keys
            in the passed-in projection. These are projections with "location": "linked"

        Projections come with multiple types: linked, and caclulated. Calculated fields are only supported
        in the data (not at the top-level attrs).

        Calculated fields in projections schema contain a callable field. This should be expressed in
        the familiar 'module:func' syntax borrowed from python entry-points.

        All projections with "location"="configuration" will look in the start document
        for metadata. Each field will be added to the return Dataset's attrs dictionary keyed
        on projection key.


        All projections with "location"="event" will look for 

        Parameters
        ----------
        run : BlueskyRun
            [description]
        projection_name : str, optional
            [description], by default None
        projection : dict, optional
            [description], by default None

        Returns
        -------
        [type]
            [description]

        Raises
        ------
        ProjectionError
            [description]
        KeyError
            [description]
        ProjectionError
            [description]
        """
        try:
            if projection is None:
                projection = Projector.get_run_projection(run, projection_name)
            if projection is None:
                raise ProjectionError("Projection could not be found")

            attrs = {}
            data_vars = {}
            for field_key, mapping in projection['projection'].items():
                # populate projection_field with calculated or  
                projection_type = mapping['type']
                projection_location = mapping.get('location')
                projection_data = None
                projection_linked_field = mapping.get('field')

                # single value data that will go in the top 
                # dataset's attributes
                if projection_location == 'configuration':
                    attrs[field_key] = run.metadata['start'][projection_linked_field]
                    continue

                # multi-dimensional data, added to return Dataset via data_vars dict
                if projection_type == "calculated":
                    data_vars[field_key] = get_calculated_value(run, field_key, mapping)
                    continue

                if projection_location == 'event':
                    projection_stream = mapping.get('stream')
                    if projection_stream is None:
                        raise ProjectionError('stream missing for event projection: {field_key}')
                    data_vars[field_key] = run[projection_stream].to_dask()[projection_linked_field]

                elif projection_location == 'configuration':
                    attrs[field_key] = projection_data
                else:
                    raise KeyError(f'Unknown location: {projection_location} in projection.')

        except Exception as e:
            raise ProjectionError('Error projecting run {e.msg}') from e
        return xarray.Dataset(data_vars, attrs=attrs)


def get_calculated_value(run: BlueskyRun, key: str, mapping: dict):
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
