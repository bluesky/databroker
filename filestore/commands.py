from __future__ import absolute_import, division, print_function

from mongoengine import connect

from .odm_templates import Resource, ResourceAttributes, Datum, ALIAS
from .retrieve import get_data as _get_data
from . import conf
from functools import wraps


def db_connect(func):
    @wraps(func)
    def inner(*args, **kwargs):
        database = conf.connection_config['database']
        host = conf.connection_config['host']
        port = conf.connection_config['port']
        connect(db=database, host=host, port=port, alias=ALIAS)
        return func(*args, **kwargs)
    return inner


@db_connect
def insert_resource(spec, resource_path, resource_kwargs=None):
    """
    Parameters
    ----------

    spec : str
        spec used to determine what handler to use to open this
        resource.

    resource_path : str or None
        Url to the physical location of this resource

    resource_kwargs : dict
        resource_kwargs name/value pairs of additional kwargs to be
        passed to the handler to open this resource.

    """
    if resource_path is None:
        resource_path = ''
    resource_object = Resource(spec=spec, resource_path=resource_path,
                               resource_kwargs=resource_kwargs)

    resource_object.save(validate=True, write_concern={"w": 1})

    return resource_object


@db_connect
def insert_resourse_attributes(resource, shape, dtype, **kwargs):
    """

    This is to be considered provisional.  The API may change drastically
    in the near future.


    kwargs
    ------


    """

    resource_attributes = ResourceAttributes(resource=resource.id, shape=shape,
                                           dtype=dtype)
    for k in ['total_bytes', 'hashed_data', 'last_access',
              'datetime_last_access', 'in_use',
              'custom_attributes']:
        v = kwargs.pop(k, None)
        if v:
            setattr(resource_attributes, v)

    if kwargs:
        raise AttributeError(kwargs.keys() +
                             '  field(s) are not among attribute keys. '
                             ' Use resource_kwargs attributes'
                             ' dict for saving it')
    resource_attributes.save(validate=True, write_concern={"w": 1})

    return resource_attributes


@db_connect
def insert_datum(resource, datum_id, datum_kwargs=None):
    """

    Parameters
    ----------

    resource : Resource on Resource.id
        Resource object

    datum_id : str
        Unique identifier for this datum.  This is the value stored in
        metadatastore and is the value passed to `retrieve` to get
        the data back out.

    datum_kwargs : dict
        dict with any kwargs needed to retrieve this specific datum from the
        resource.

    """

    datum = Datum(resource=resource, datum_id=datum_id,
                  datum_kwargs=datum_kwargs)
    datum.save(validate=True, write_concern={"w": 1})

    return datum


@db_connect
def find_resource_attributes(resource):
    """Return resource_attributes entry given a resource object


    This is to be considered provisional.  The API may change drastically
    in the near future.


    Parameters
    ----------
    resource: filestore.database.resource.Resource
        Resource object

    """

    return ResourceAttributes.objects(resource=resource.id)


@db_connect
def retrieve(eid):
    """
    Given a resource identifier return the data.

    Parameters
    ----------
    eid : str
        The resource ID (as stored in MDS)

    Returns
    -------
    data : ndarray
        The requested data as a numpy array
    """
    edoc = Datum.objects.get(datum_id=eid)
    return _get_data(edoc)
