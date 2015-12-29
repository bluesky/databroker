from __future__ import absolute_import, division, print_function

from mongoengine import connect
import mongoengine.connection
from bson import ObjectId
from .odm_templates import Resource, Datum, ALIAS
from .retrieve import get_data as _get_data
from . import conf
from functools import wraps

from .odm_templates import known_spec

from .core import (bulk_insert_datum as _bulk_insert_datum,
                   insert_datum as _insert_datum,
                   insert_resource as _insert_resource)


def _ensure_connection(func):
    @wraps(func)
    def inner(*args, **kwargs):
        database = conf.connection_config['database']
        host = conf.connection_config['host']
        port = int(conf.connection_config['port'])
        db_connect(database=database, host=host, port=port)
        return func(*args, **kwargs)
    return inner


def db_disconnect():
    mongoengine.connection.disconnect(ALIAS)
    Datum._collection = None
    Resource._collection = None


def db_connect(database, host, port):
    return connect(db=database, host=host, port=port, alias=ALIAS)


@_ensure_connection
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
    col = Resource._get_collection()

    if resource_path is None:
        resource_path = ''
    if resource_kwargs is None:
        resource_kwargs = {}

    return _insert_resource(col, spec, resource_path, resource_kwargs,
                            known_spec)


@_ensure_connection
def insert_datum(resource, datum_id, datum_kwargs=None):
    """

    Parameters
    ----------

    resource : Resource or Resource.id
        Resource object

    datum_id : str
        Unique identifier for this datum.  This is the value stored in
        metadatastore and is the value passed to `retrieve` to get
        the data back out.

    datum_kwargs : dict
        dict with any kwargs needed to retrieve this specific datum from the
        resource.

    """
    col = Datum._get_collection()

    try:
        resource['spec']
    except (AttributeError, TypeError):
        res_col = Resource._get_collection()
        resource = res_col.find_one({'_id': ObjectId(resource)})
        resource['id'] = resource['_id']
    if datum_kwargs is None:
        datum_kwargs = {}

    return _insert_datum(col, resource, datum_id, datum_kwargs,
                         known_spec)


@_ensure_connection
def bulk_insert_datum(resource, datum_ids, datum_kwarg_list):
    col = Datum._get_collection()
    return _bulk_insert_datum(col, resource, datum_ids, datum_kwarg_list)


@_ensure_connection
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
    return _get_data(eid)
