from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from bson import ObjectId
from bson.errors import InvalidId
import six
import pymongo
from collections import deque
from .core import (DatumNotFound, _get_datum_from_datum_id, retrieve,
                   resource_given_datum_id, insert_datum, insert_resource,
                   update_resource, get_datum_by_res_gen, get_file_list,
                   bulk_register_datum_table, register_datum)


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
    return ret


def bulk_insert_datum(col, resource, datum_ids,
                      datum_kwarg_list):

    resource_id = doc_or_uid_to_uid(resource)

    def datum_factory():
        for d_id, d_kwargs in zip(datum_ids, datum_kwarg_list):
            datum = dict(resource=resource_id,
                         datum_id=str(d_id),
                         datum_kwargs=dict(d_kwargs))
            yield datum

    d_uids = deque()
    bulk = []
    for dm in datum_factory():
        bulk.append(pymongo.InsertOne(dm))
        d_uids.append(dm['datum_id'])
    col.bulk_write(bulk, ordered=False)
    return d_uids


def get_resource_history(col, resource):
    uid = doc_or_uid_to_uid(resource)
    cursor = col.find({'resource': uid}).sort('time')
    for doc in cursor:
        for k in ['new', 'old']:
            d = doc[k]
            d.pop('_id', None)
            d['id'] = d['uid']
            doc[k] = d
        doc.pop('_id')
        yield doc
