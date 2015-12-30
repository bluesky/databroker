from __future__ import absolute_import, division, print_function


from .retrieve import get_data as _get_data, _FS_SINGLETON


def db_disconnect():
    _FS_SINGLETON.disconnect()


def db_connect(database, host, port):
    _FS_SINGLETON.reconfigure(dict(database=database,
                                   host=host,
                                   port=port))
    assert _FS_SINGLETON.config['database'] == database
    return _FS_SINGLETON._connection


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
    resource_kwargs = resource_kwargs if resource_kwargs is not None else {}
    return _FS_SINGLETON.insert_resource(spec, resource_path, resource_kwargs)


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
    datum_kwargs = datum_kwargs if datum_kwargs is not None else {}
    return _FS_SINGLETON.insert_datum(resource, datum_id, datum_kwargs)


def bulk_insert_datum(resource, datum_ids, datum_kwarg_list):
    return _FS_SINGLETON.bulk_insert_datum(resource, datum_ids,
                                           datum_kwarg_list)


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
