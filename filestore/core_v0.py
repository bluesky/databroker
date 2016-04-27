from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from doct import Document
from jsonschema import validate as js_validate
from bson import ObjectId
from .core import DatumNotFound
import os.path


def get_datum(col, eid, datum_cache, get_spec_handler, logger):
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

    handler = get_spec_handler(datum['resource'])
    return handler(**datum['datum_kwargs'])


def bulk_insert_datum(col, resource, datum_ids, datum_kwarg_list):

    resource_id = ObjectId(resource['id'])

    def datum_factory():
        for d_id, d_kwargs in zip(datum_ids, datum_kwarg_list):
            datum = dict(resource=resource_id,
                         datum_id=str(d_id),
                         datum_kwargs=dict(d_kwargs))
            yield datum

    bulk = col.initialize_ordered_bulk_op()
    for dm in datum_factory():
        bulk.insert(dm)

    return bulk.execute()


def insert_datum(col, resource, datum_id, datum_kwargs, known_spec,
                 resource_col):
    try:
        resource['spec']
    except (AttributeError, TypeError):
        resource = resource_col.find_one({'_id': ObjectId(resource)})
        resource['id'] = resource['_id']

    spec = resource['spec']
    if spec in known_spec:
        js_validate(datum_kwargs, known_spec[spec]['datum'])
    datum = dict(resource=ObjectId(resource['id']),
                 datum_id=str(datum_id),
                 datum_kwargs=dict(datum_kwargs))

    col.insert_one(datum)
    # do not leak mongo objectID
    datum.pop('_id', None)

    return Document('datum', datum)


def insert_resource(col, spec, resource_path, resource_kwargs,
                    known_spec, chroot=''):
    resource_kwargs = dict(resource_kwargs)
    if spec in known_spec:
        js_validate(resource_kwargs, known_spec[spec]['resource'])

    if chroot:
        resource_path = os.path.join(chroot, resource_path)
    resource_object = dict(spec=str(spec),
                           resource_path=str(resource_path),
                           resource_kwargs=resource_kwargs)

    col.insert_one(resource_object)
    # rename to play nice with ME
    resource_object['id'] = resource_object.pop('_id')
    return resource_object
