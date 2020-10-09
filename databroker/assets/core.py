from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from jsonschema import validate as js_validate
import warnings
import uuid
import time as ttime
import pandas as pd
from ..utils import sanitize_np, apply_to_dict_recursively


class DatumNotFound(Exception):
    """
    Raised if a Datum id is not found.
    """
    def __init__(self, datum_id, msg=None, *args):
        if msg is None:
            msg = f"No datum found with datum id {datum_id}"
        super().__init__(msg, *args)
        self.datum_id = datum_id


class EventDatumNotFound(Exception):
    """
    Raised if an Event document is found to have an unknown Datum id.
    """
    def __init__(self, event_uid, datum_id, msg=None, *args):
        if msg is None:
            msg = (
                f"Event with uid {event_uid} references "
                f"unknown Datum with datum id {datum_id}"
            )
        super().__init__(msg, *args)
        self.event_uid = event_uid
        self.datum_id = datum_id


def doc_or_uid_to_uid(doc_or_uid):
    """Given Document or uid return the uid

    Parameters
    ----------
    doc_or_uid : dict or str
        If str, then assume uid and pass through, if not, return
        the 'uid' field

    Returns
    -------
    uid : str
        A string version of the uid of the given document

    """
    if not isinstance(doc_or_uid, six.string_types):
        doc_or_uid = doc_or_uid['uid']
    return doc_or_uid


def _get_datum_from_datum_id(col, datum_id, datum_cache, logger):
    try:
        datum = datum_cache[datum_id]
    except KeyError:
        # find the current document
        edoc = col.find_one({'datum_id': datum_id})
        if edoc is None:
            raise DatumNotFound(datum_id=datum_id)
        # save it for later
        datum = dict(edoc)

        res = edoc['resource']
        count = 0
        for dd in col.find({'resource': res}):
            count += 1
            d_id = dd['datum_id']
            if d_id not in datum_cache:
                datum_cache[d_id] = dict(dd)
        if count > datum_cache.max_size:
            logger.warn("More datum in a resource than your "
                        "datum cache can hold.")

    datum.pop('_id', None)
    return datum


def retrieve(col, datum_id, datum_cache, get_spec_handler, logger):
    datum = _get_datum_from_datum_id(col, datum_id, datum_cache, logger)
    handler = get_spec_handler(datum['resource'])
    return handler(**datum['datum_kwargs'])


def resource_given_datum_id(col, datum_id, datum_cache, logger):
    datum_id = doc_or_uid_to_uid(datum_id)
    datum = _get_datum_from_datum_id(col, datum_id, datum_cache, logger)
    res = datum['resource']
    return res


def resource_given_uid(col, resource):
    uid = doc_or_uid_to_uid(resource)
    ret = col.find_one({'uid': uid})
    ret.pop('_id', None)
    ret['id'] = ret['uid']
    return ret


def bulk_insert_datum(col, resource, datum_ids,
                      datum_kwarg_list):

    resource_id = doc_or_uid_to_uid(resource)

    def datum_factory():
        for d_id, d_kwargs in zip(datum_ids, datum_kwarg_list):
            datum = dict(resource=resource_id,
                         datum_id=str(d_id),
                         datum_kwargs=dict(d_kwargs))
            apply_to_dict_recursively(datum, sanitize_np)
            yield datum

    col.insert(datum_factory())


def bulk_register_datum_table(datum_col,
                              resource_uid,
                              dkwargs_table,
                              validate):
    if validate:
        raise

    d_ids = [str(uuid.uuid4()) for j in range(len(dkwargs_table))]
    dkwargs_table = pd.DataFrame(dkwargs_table)
    bulk_insert_datum(datum_col, resource_uid, d_ids, [
        dict(r) for _, r in dkwargs_table.iterrows()])
    return d_ids


def register_datum(col, resource_uid, datum_kwargs):
    datum_uid = str(uuid.uuid4())
    datum = insert_datum(col, resource_uid, datum_uid, datum_kwargs, {}, None)
    return datum['datum_id']


def insert_datum(col, resource, datum_id, datum_kwargs, known_spec,
                 resource_col, ignore_duplicate_error=False,
                 duplicate_exc=None):
    if ignore_duplicate_error:
        assert duplicate_exc is not None

    if duplicate_exc is None:
        class _PrivateException(Exception):
            pass
        duplicate_exc = _PrivateException
    try:
        resource['spec']
        spec = resource['spec']

        if spec in known_spec:
            js_validate(datum_kwargs, known_spec[spec]['datum'])
    except (AttributeError, TypeError):
        pass

    resource_uid = doc_or_uid_to_uid(resource)

    datum = dict(resource=resource_uid,
                 datum_id=str(datum_id),
                 datum_kwargs=dict(datum_kwargs))
    apply_to_dict_recursively(datum, sanitize_np)
    # We are transitioning from ophyd objects inserting directly into a
    # Registry to ophyd objects passing documents to the RunEngine which in
    # turn inserts them into a Registry. During the transition period, we allow
    # an ophyd object to attempt BOTH so that configuration files are
    # compatible with both the new model and the old model. Thus, we need to
    # ignore the second attempt to insert.
    try:
        col.insert_one(datum)
    except duplicate_exc:
        if ignore_duplicate_error:
            warnings.warn("Ignoring attempt to insert Resource with duplicate "
                          "uid, assuming that both ophyd and bluesky "
                          "attempted to insert this document. Remove the "
                          "Registry (`reg` parameter) from your ophyd "
                          "instance to remove this warning.")
        else:
            raise
    # do not leak mongo objectID
    datum.pop('_id', None)

    return datum


def insert_resource(col, spec, resource_path, resource_kwargs,
                    known_spec, root, path_semantics='posix', uid=None,
                    run_start=None, id=None,
                    ignore_duplicate_error=False, duplicate_exc=None):
    """Insert resource into a databroker.

    Parameters
    ----------
    col : pymongo.Collection instance
        Collection to insert data into
    spec : str
        The resource data spec
    resource_path : str
        The path to the resource files
    resource_kwargs : dict
        The kwargs for the resource
    known_spec : set
        The known specs
    root : str
        The root of the file path
    path_semantics : str, optional
        The name of the path semantics, e.g. ``posix`` for Linux systems
    uid : str, optional
        The unique ID for the resource
    run_start : str, optional
        The unique ID for the start document the resource is associated with
    id : str, optional
        Dummy variable so that we round trip resources, same as ``uid``

    Returns
    -------
    resource_object : dict
        The resource
    """
    if ignore_duplicate_error:
        assert duplicate_exc is not None
    if duplicate_exc is None:
        class _PrivateException(Exception):
            pass
        duplicate_exc = _PrivateException
    resource_kwargs = dict(resource_kwargs)
    if spec in known_spec:
        js_validate(resource_kwargs, known_spec[spec]['resource'])
    if uid is None:
        uid = str(uuid.uuid4())

    resource_object = dict(spec=str(spec),
                           resource_path=str(resource_path),
                           root=str(root),
                           resource_kwargs=resource_kwargs,
                           path_semantics=path_semantics,
                           uid=uid)
    # This is special-cased because it was added later.
    # Someday this may be required and no longer special-cased.
    if run_start is not None:
        resource_object['run_start'] = run_start
    # We are transitioning from ophyd objects inserting directly into a
    # Registry to ophyd objects passing documents to the RunEngine which in
    # turn inserts them into a Registry. During the transition period, we allow
    # an ophyd object to attempt BOTH so that configuration files are
    # compatible with both the new model and the old model. Thus, we need to
    # ignore the second attempt to insert.
    try:
        col.insert_one(resource_object)
    except duplicate_exc:
        if ignore_duplicate_error:
            warnings.warn("Ignoring attempt to insert Datum with duplicate "
                          "datum_id, assuming that both ophyd and bluesky "
                          "attempted to insert this document. Remove the "
                          "Registry (`reg` parameter) from your ophyd "
                          "instance to remove this warning.")
        else:
            raise
    resource_object['id'] = resource_object['uid']
    resource_object.pop('_id', None)
    return resource_object


def update_resource(update_col, resource_col, old, new, cmd, cmd_kwargs):
    '''Update a resource document

    Parameters
    ----------
    update_col : Collection
        The collection to record audit trail in
    resource_col : Collection
        The resource collection

    old : dict
        The old resource document

    new : dict
        The new resource document

    cmd : str
        The name of the operation which generated this update

    cmd_kwargs : dict
        The arguments that went into the update (excluding the resource id)


    Returns
    -------
    ret : dict
        The new resource document

    log_object : dict
        The history object inserted (with oid removed)
    '''
    if old['uid'] != new['uid']:
        raise RuntimeError('must not change the resource uid')
    uid = old['uid']
    log_object = {'resource': uid,
                  'old': old,
                  'new': new,
                  'time': ttime.time(),
                  'cmd': cmd,
                  'cmd_kwargs': cmd_kwargs}

    update_col.insert_one(log_object)
    result = resource_col.replace_one({'uid': uid}, new)
    ret = resource_given_uid(resource_col, uid)
    # TODO look inside of result
    del result
    log_object.pop('_id', None)
    return ret, log_object


def get_resource_history(col, resource):
    uid = doc_or_uid_to_uid(resource)
    cursor = col.find({'resource': uid})
    for doc in cursor:
        for k in ['new', 'old']:
            d = doc[k]
            d.pop('_id', None)
            d['id'] = d['uid']
            doc[k] = d
        doc.pop('_id', None)
        yield doc


def get_datumkw_by_resuid_gen(datum_col, resource_uid):
    '''Given a resource uid, get all datum_kwargs

    No order is guaranteed.

    Internally the result of this is passed to the `get_file_list` method
    of the handler object in `change_root`

    Parameters
    ----------
    datum_col : Collection
        The Datum collection

    resource_uid : dict or str
       The resource to work on

    Yields
    ------
    datum_kwarg : dict
    '''
    resource_uid = doc_or_uid_to_uid(resource_uid)
    cur = datum_col.find({'resource': resource_uid})

    for d in cur:
        yield d['datum_kwargs']


def get_datum_by_res_gen(datum_col, resource_uid):
    '''Given a resource uid, get all datums

    No order is guaranteed.

    Internally the result of this is passed to the `get_file_list` method
    of the handler object in `change_root`

    Parameters
    ----------
    datum_col : Collection
        The Datum collection

    resource_uid : dict or str
       The resource to work on

    Yields
    ------
    datum : dict
    '''
    resource_uid = doc_or_uid_to_uid(resource_uid)
    cur = datum_col.find({'resource': resource_uid})

    for d in cur:
        yield d


def get_file_list(resource, datum_kwarg_gen, get_spec_handler):
    """
    Given a resource and an iterable of datum kwargs, get a list of
    associated files.

    DO NOT USE FOR COPYING OR MOVING. This is for debugging only.
    See the methods for moving and copying on the Registry object.
    """
    handler = get_spec_handler(resource['uid'])
    return handler.get_file_list(datum_kwarg_gen)
