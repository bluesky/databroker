from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from bson import ObjectId
from bson.errors import InvalidId
import six
from doct import Document
from jsonschema import validate as js_validate
import uuid
import time as ttime
import pymongo
from .core import (DatumNotFound, )


DuplicateKeyError = pymongo.errors.DuplicateKeyError


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
        try:
            doc_or_uid = doc_or_uid['uid']
        except TypeError:
            pass
    return doc_or_uid


def _get_datum_from_eid(col, eid, datum_cache, logger):
    try:
        datum = datum_cache[eid]
    except KeyError:
        keys = ['datum_kwargs', 'resource']
        # find the current document
        edoc = col.find_one({'datum_id': eid})
        if edoc is None:
            raise DatumNotFound(
                "No datum found with datum_id {!r}".format(eid))
        # save it for later
        datum = {k: edoc[k] for k in keys}

        res = edoc['resource']
        count = 0
        for dd in col.find({'resource': res}):
            count += 1
            d_id = dd['datum_id']
            if d_id not in datum_cache:
                datum_cache[d_id] = {k: dd[k] for k in keys}
        if count > datum_cache.max_size:
            logger.warn("More datum in a resource than your "
                        "datum cache can hold.")

    return datum


def retrieve(col, eid, datum_cache, get_spec_handler, logger):
    datum = _get_datum_from_eid(col, eid, datum_cache, logger)
    handler = get_spec_handler(datum['resource'])
    return handler(**datum['datum_kwargs'])


def resource_given_eid(col, eid, datum_cache, logger):
    datum = _get_datum_from_eid(col, eid, datum_cache, logger)
    return datum['resource']


def resource_given_uid(col, resource):
    uid = doc_or_uid_to_uid(resource)

    try:
        uid = ObjectId(uid)
    except InvalidId:
        ret = col.find_one({'uid': uid})
    else:
        ret = col.find_one({'_id': uid})

    if ret is None:
        raise RuntimeError('did not find resource {!r}'.format(resource))

    oid = ret.pop('_id')
    ret.setdefault('uid', oid)
    ret['id'] = ret['uid']
    return Document('resource', ret)


def bulk_insert_datum(col, resource, datum_ids,
                      datum_kwarg_list):

    resource_id = doc_or_uid_to_uid(resource)

    def datum_factory():
        for d_id, d_kwargs in zip(datum_ids, datum_kwarg_list):
            datum = dict(resource=resource_id,
                         datum_id=str(d_id),
                         datum_kwargs=dict(d_kwargs))
            yield datum

    bulk = col.initialize_unordered_bulk_op()
    for dm in datum_factory():
        bulk.insert(dm)

    return bulk.execute()


def insert_datum(col, resource, datum_id, datum_kwargs, known_spec,
                 resource_col):
    try:
        resource['spec']
    except (AttributeError, TypeError):
        resource = resource_col.find_one({'uid': resource})

    spec = resource['spec']

    if spec in known_spec:
        js_validate(datum_kwargs, known_spec[spec]['datum'])

    datum = dict(resource=resource['uid'],
                 datum_id=str(datum_id),
                 datum_kwargs=dict(datum_kwargs))

    col.insert_one(datum)
    # do not leak mongo objectID
    datum.pop('_id', None)

    return Document('datum', datum)


def insert_resource(col, spec, resource_path, resource_kwargs,
                    known_spec, root):
    resource_kwargs = dict(resource_kwargs)
    if spec in known_spec:
        js_validate(resource_kwargs, known_spec[spec]['resource'])

    resource_object = dict(spec=str(spec),
                           resource_path=str(resource_path),
                           root=str(root),
                           resource_kwargs=resource_kwargs,
                           uid=str(uuid.uuid4()))

    col.insert_one(resource_object)
    # maintain back compatibility
    resource_object['id'] = resource_object['uid']
    resource_object.pop('_id')
    return Document('resource', resource_object)


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
    ret : Document
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
    log_object.pop('_id')
    return ret, log_object


def get_resource_history(col, resource):
    uid = doc_or_uid_to_uid(resource)
    cursor = col.find({'resource': uid}).sort('time')
    for doc in cursor:
        for k in ['new', 'old']:
            d = doc[k]
            d.pop('_id', None)
            d['id'] = d['uid']
            doc[k] = Document('resource', d)
        doc.pop('_id')
        yield Document('update', doc)


def get_datum_by_res_gen(datum_col, resource_uid):
    '''Given a resource uid, get all datums

    No order is guaranteed.

    Internally the result of this is passed to the `get_file_list` method
    of the handler object in `change_root`

    Parameters
    ----------
    datam_col : Collection
        The Datum collection

    resource_uid : Document or str
       The resource to work on

    Yields
    ------
    datum : doct.Document
    '''
    resource_uid = doc_or_uid_to_uid(resource_uid)
    cur = datum_col.find({'resource': resource_uid})

    for d in cur:
        yield Document('datum', d)


def get_file_list(resource, datum_kwarg_gen, get_spec_handler):
    """
    Given a resource and an iterable of datum kwargs, get a list of
    associated files.

    DO NOT USE FOR COPYING OR MOVING. This is for debugging only.
    See the methods for moving and copying on the FileStore object.
    """
    handler = get_spec_handler(resource['uid'])
    return handler.get_file_list(datum_kwarg_gen)
