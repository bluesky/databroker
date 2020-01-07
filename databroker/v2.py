import event_model
import tempfile

from .core import parse_handler_registry, discover_handlers, parse_transforms
from intake.catalog import Catalog
from event_model import DuplicateHandler
from functools import partial
from pathlib import Path


class Broker(Catalog):
    """
    This is a thin wrapper around intake.Catalog.

    It includes an accessor the databroker API version 1.

    Parameters
    ----------
    handler_registry: dict, optional
        This is passed to the Filler or whatever class is given in the
        filler_class parameter below.

        Maps each 'spec' (a string identifying a given type or external
        resource) to a handler class.

        A 'handler class' may be any callable with the signature::

            handler_class(resource_path, root, **resource_kwargs)

        It is expected to return an object, a 'handler instance', which is also
        callable and has the following signature::

            handler_instance(**datum_kwargs)

        As the names 'handler class' and 'handler instance' suggest, this is
        typically implemented using a class that implements ``__init__`` and
        ``__call__``, with the respective signatures. But in general it may be
        any callable-that-returns-a-callable.
    root_map: dict, optional
        This is passed to Filler or whatever class is given in the filler_class
        parameter below.

        str -> str mapping to account for temporarily moved/copied/remounted
        files.  Any resources which have a ``root`` in ``root_map`` will be
        loaded using the mapped ``root``.
    filler_class: type
        This is Filler by default. It can be a Filler subclass,
        ``functools.partial(Filler, ...)``, or any class that provides the same
        methods as ``DocumentRouter``.
    transforms: dict
        A dict that maps any subset of the keys {start, stop, resource, descriptor}
        to a function that accepts a document of the corresponding type and
        returns it, potentially modified. This feature is for patching up
        erroneous metadata. It is intended for quick, temporary fixes that
        may later be applied permanently to the data at rest
        (e.g., via a database migration).
    **kwargs :
        Additional keyword arguments are passed through to the base class,
        Catalog.
    """

    def __init__(self, *, handler_registry=None, root_map=None,
                 filler_class=event_model.Filler, transforms=None, **kwargs):

        self._root_map = root_map
        self._filler_class = filler_class
        self._transforms = parse_transforms(transforms)
        if handler_registry is None:
            handler_registry = discover_handlers()
        self._handler_registry = parse_handler_registry(handler_registry)
        self.handler_registry = event_model.HandlerRegistryView(
            self._handler_registry)

        self._get_filler = partial(self._filler_class,
                                   handler_registry=self.handler_registry,
                                   root_map=self._root_map,
                                   inplace=False)

        super().__init__(**kwargs)

    @property
    def root_map(self):
        # This is mutable, so be advised that users *can* mutate it under us.
        # The property just prohibits them from setting it to an entirely
        # different dict instance.
        return self._root_map

    @property
    def v1(self):
        "Accessor to the version 1 API."
        if not hasattr(self, '_Broker__v1'):
            from .v1 import Broker
            self.__v1 = Broker(self)
        return self.__v1

    @property
    def v2(self):
        "A self-reference. This makes v1.Broker and v2.Broker symmetric."
        return self

    def register_handler(self, spec, handler, overwrite=False):
        if (not overwrite) and (spec in self._handler_registry):
            original = self._handler_registry[spec]
            if original is handler:
                return
            raise DuplicateHandler(
                f"There is already a handler registered for the spec {spec!r}. "
                f"Use overwrite=True to deregister the original.\n"
                f"Original: {original}\n"
                f"New: {handler}")
        self._handler_registry[spec] = handler

    def deregister_handler(self, spec):
        self._handler_registry.pop(spec, None)


def temp():
    """
    Generate a Catalog backed by a temporary directory of msgpack-encoded files.
    """
    from databroker._drivers.msgpack import BlueskyMsgpackCatalog
    handler_registry = {}
    # Let ophyd be an optional dependency.
    # If it is not installed, then we clearly do not need its handler for this
    # temporary data store.
    try:
        import ophyd.sim
    except ImportError:
        pass
    else:
        handler_registry['NPY_SEQ'] = ophyd.sim.NumpySeqHandler
    tmp_dir = tempfile.mkdtemp()
    tmp_data_dir = Path(tmp_dir) / 'data'
    catalog = BlueskyMsgpackCatalog(
        f"{tmp_data_dir}/*.msgpack",
        name='temp',
        handler_registry=handler_registry)
    return catalog
