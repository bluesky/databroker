from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from document import Document
from jsonschema import validate as js_validate
from bson import ObjectId


class DatumNotFound(Exception):
    pass


def get_datum(col, eid, _DATUM_CACHE, get_spec_handler, logger):
    try:
        datum = _DATUM_CACHE[eid]
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
            if d_id not in _DATUM_CACHE:
                _DATUM_CACHE[d_id] = {k: dd[k] for k in keys}
        if count > _DATUM_CACHE.max_size:
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


def insert_datum(col, resource, datum_id, datum_kwargs, known_spec):
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
                    known_spec):
    resource_kwargs = dict(resource_kwargs)
    if spec in known_spec:
        js_validate(resource_kwargs, known_spec[spec]['resource'])

    resource_object = dict(spec=str(spec),
                           resource_path=str(resource_path),
                           resource_kwargs=resource_kwargs)

    col.insert_one(resource_object)
    # rename to play nice with ME
    resource_object['id'] = resource_object.pop('_id')
    return resource_object
