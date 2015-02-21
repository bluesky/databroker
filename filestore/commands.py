from __future__ import absolute_import, division, print_function

from mongoengine import connect

from .odm_templates import Resource, ResoureAttributes, Nugget
from .retrieve import get_data as _get_data
from . import conf
from functools import wraps

def db_connect(func):
    @wraps(func)
    def inner(*args, **kwargs):
        database = conf.connection_config['database']
        host = conf.connection_config['host']
        port = conf.connection_config['port']
        connect(db=database, host=host, port=port)
        return func(*args, **kwargs)
    return inner


@db_connect
def insert_resource(spec, file_path, custom=None):
    """
    Parameters
    ----------

    spec: str
        File spec used to primarily parse the contents into
        analysis environment

    file_path: str
        Url to the physical location of the file

    custom: dict
        custom name/value container in case additional info save is required

    """

    resource_object = Resource(spec=spec, file_path=file_path,
                               custom=custom)

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

    resoure_attributes = ResoureAttributes(resource=resource.id, shape=shape,
                                     dtype=dtype,
                                     collection_version=collection_version)

    resoure_attributes.total_bytes = kwargs.pop('total_bytes', None)
    resoure_attributes.hashed_data = kwargs.pop('hashed_data', None)
    resoure_attributes.last_access = kwargs.pop('last_access', None)
    resoure_attributes.datetime_last_access = kwargs.pop('datetime_last_access',
                                                      None)
    resoure_attributes.in_use = kwargs.pop('in_use', None)
    resoure_attributes.custom_attributes = kwargs.pop('custom_attributes', None)

    if kwargs:
        raise AttributeError(kwargs.keys() +
                             '  field(s) are not among attribute keys. '
                             ' Use custom attributes'
                             ' dict for saving it')
    resoure_attributes.save(validate=True, write_concern={"w": 1})

    return resoure_attributes


@db_connect
def insert_nugget(resource, event_id,
                  link_parameters=None):
    """

    Parameters
    ----------

    resource: filestore.database.resource.Resource
        Resource object

    event_id: str
        metadataStore unique event identifier in string format

    link_parameters: dict
        custom dict required for appending name/value pairs as desired

    """

    nugget = Nugget(resource=resource.id,
                    event_id=event_id,
                    link_parameters=link_parameters)
    nugget.save(validate=True, write_concern={"w": 1})

    return nugget


@db_connect
def find_resource(**kwargs):

    query_dict = dict()

    try:
        query_dict['spec'] = kwargs.pop('spec')
    except:
        pass

    try:
        query_dict['file_path'] = kwargs.pop('file_path')
    except:
        pass

    resource_objects = Resource.objects(__raw__=query_dict).order_by('-_id')

    return resource_objects


@db_connect
def find_nugget(resource=None, event_id=None):

    query_dict = dict()

    if resource is not None:
        query_dict['resource'] = resource.id
    elif event_id is not None:
        query_dict['event_id'] = event_id
    else:
        raise AttributeError(
            'Search parameters are invalid. resource '
            'or event_id search is possible')

    return Nugget.objects(__raw__=query_dict)


@db_connect
def find_resoure_attributes(resource):
    """Return  resoure_attributes entry given a file_header object


    This is to be considered provisional.  The API may change drastically
    in the near future.


    Parameters
    ----------
    resource: filestore.database.resource.Resource
        Resource object

    """

    return ResoureAttributes.objects(resource=resource.id)


def retrieve_data(eid):
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
    edocs = find_nugget(event_id=eid)
    # TODO add sanity checks
    if edocs:
        return _get_data(edocs[0])
    else:
        raise ValueError("none found")
