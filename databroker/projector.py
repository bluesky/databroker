import xarray

from .core import BlueskyRun


class ProjectionError(Exception):
    pass


class Projector():
    def __init__(self, run: BlueskyRun):
        self._run = run

    def project(self, projection: dict):
        pass


class XarrayProjector(Projector):
    def __init__(self, run: BlueskyRun):
        super().__init__(run)

    def project(self, projection: dict):
        try:
            # get 
            configuration = self._run.metadata['start']
            attrs = {}
            data_vars = {}
            for field, mapping in projection.items():
                location = mapping['location']
                field = mapping['field']
                if location == 'event':
                    stream = mapping['stream']
                    data_vars[field] = self.run[stream].to_dask()[field]
                elif location == 'configuration':
                    attrs[field] = configuration[field]
                else:
                    raise KeyError(f'Unknown location: {location} in projection.')
        except Exception as e:
            raise ProjectionError('Error with projecting run', e)
        return xarray.Dataset(data_vars, attrs=attrs)