from __future__ import absolute_import, division, print_function

from mongoengine import connect
import mongoengine.connection
from bson import ObjectId
from .odm_templates import Resource, Datum, ALIAS
from .retrieve import get_data as _get_data
from . import conf
from functools import wraps

from .odm_templates import known_spec
from jsonschema import validate as js_validate

from document import Document


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
    if spec in known_spec:
        js_validate(resource_kwargs, known_spec[spec]['resource'])

    resource_object = dict(spec=spec, resource_path=resource_path,
                           resource_kwargs=resource_kwargs)

    col.insert_one(resource_object)
    # rename to play nice with ME
    resource_object['id'] = resource_object.pop('_id')
    return resource_object


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
        spec = resource['spec']
    except (AttributeError, TypeError):
        res_col = Resource._get_collection()
        resource = res_col.find_one({'_id': ObjectId(resource)})
        spec = resource['spec']
        resource['id'] = resource['_id']
    if datum_kwargs is None:
        datum_kwargs = {}
    if spec in known_spec:
        js_validate(datum_kwargs, known_spec[spec]['datum'])
    datum = dict(resource=resource['id'], datum_id=datum_id,
                 datum_kwargs=datum_kwargs)
    col.insert_one(datum)
    datum.pop('_id')

    return Document('datum', datum)


@_ensure_connection
def bulk_insert_datum(resource, datum_ids, datum_kwarg_list):

    resource_id = resource['id']

    def datum_factory():
        for d_id, d_kwargs in zip(datum_ids, datum_kwarg_list):
            datum = dict(resource=resource_id,
                         datum_id=d_id,
                         datum_kwargs=d_kwargs)
            yield datum

    bulk = Datum._get_collection().initialize_ordered_bulk_op()
    for dm in datum_factory():
        bulk.insert(dm)

    return bulk.execute()


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
