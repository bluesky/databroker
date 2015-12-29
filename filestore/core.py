from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from .odm_templates import Datum


class DatumNotFound(Datum.DoesNotExist):
    pass


def get_datum(col, eid, handle_registry, _DATUM_CACHE,
              get_spec_handler, logger):
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

    handler = get_spec_handler(datum['resource'], handle_registry)
    return handler(**datum['datum_kwargs'])


def bulk_insert_datum(col, resource, datum_ids, datum_kwarg_list):

    resource_id = resource['id']

    def datum_factory():
        for d_id, d_kwargs in zip(datum_ids, datum_kwarg_list):
            datum = dict(resource=resource_id,
                         datum_id=d_id,
                         datum_kwargs=d_kwargs)
            yield datum

    bulk = col.initialize_ordered_bulk_op()
    for dm in datum_factory():
        bulk.insert(dm)

    return bulk.execute()
