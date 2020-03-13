import event_model
import glob
import msgpack
import msgpack_numpy
import os
import pathlib

from ..in_memory import BlueskyInMemoryCatalog


UNPACK_OPTIONS = dict(object_hook=msgpack_numpy.decode,
                      raw=False,
                      max_buffer_size=1_000_000_000)


def gen(filename):
    """
    A msgpack generator

    Parameters
    ----------
    filename: str
        msgpack file to laod.
    """
    with open(filename, 'rb') as file:
        yield from msgpack.Unpacker(file, **UNPACK_OPTIONS)


def get_stop(filename):
    """
    Returns the stop_doc of a Bluesky msgpack file.

    The stop_doc is always the last line of the file.

    Parameters
    ----------
    filename: str
        msgpack file to load.
    Returns
    -------
    stop_doc: dict or None
        A Bluesky run_stop document or None if one is not present.
    """
    with open(filename, 'rb') as file:
        for name, doc in msgpack.Unpacker(file, **UNPACK_OPTIONS):
            if name == 'stop':
                return doc


class BlueskyMsgpackCatalog(BlueskyInMemoryCatalog):
    name = 'bluesky-msgpack-catalog'  # noqa

    def __init__(self, paths, *,
                 handler_registry=None, root_map=None,
                 filler_class=event_model.Filler, query=None,
                 transforms=None, **kwargs):
        """
        This Catalog is backed by msgpack files.

        Each chunk the file is expected to be a list with two elements,
        the document name (type) and the document itself. The documents are
        expected to be in chronological order.

        Parameters
        ----------
        paths : list
            list of filepaths
        handler_registry : dict, optional
            This is passed to the Filler or whatever class is given in the
            ``filler_class`` parameter below.

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
            This is passed to Filler or whatever class is given in the
            ``filler_class`` parameter below.
            str -> str mapping to account for temporarily moved/copied/remounted
            files.  Any resources which have a ``root`` in ``root_map`` will be
            loaded using the mapped ``root``.
        filler_class: type, optional
            This is Filler by default. It can be a Filler subclass,
            ``functools.partial(Filler, ...)``, or any class that provides the
            same methods as ``DocumentRouter``.
        query : dict, optional
            Mongo query that filters entries' RunStart documents
        transforms : Dict[str, Callable]
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
        # Tolerate a single path (as opposed to a list).
        if isinstance(paths, (str, pathlib.Path)):
            paths = [paths]
        self.paths = paths
        self._filename_to_mtime = {}
        super().__init__(handler_registry=handler_registry,
                         root_map=root_map, filler_class=filler_class,
                         query=query, transforms=transforms, **kwargs)

    def _load(self):
        for path in self.paths:
            for filename in glob.glob(path):
                mtime = os.path.getmtime(filename)
                if mtime == self._filename_to_mtime.get(filename):
                    # This file has not changed since last time we loaded it.
                    continue
                self._filename_to_mtime[filename] = mtime
                with open(filename, 'rb') as file:
                    unpacker = msgpack.Unpacker(file, **UNPACK_OPTIONS)
                    try:
                        name, start_doc = next(unpacker)
                    except StopIteration:
                        # Empty file, maybe being written to currently
                        continue
                stop_doc = get_stop(filename)
                self.upsert(start_doc, stop_doc, gen, (filename,), {})

    def search(self, query):
        """
        Return a new Catalog with a subset of the entries in this Catalog.

        Parameters
        ----------
        query : dict
        """
        query = dict(query)
        if self._query:
            query = {'$and': [self._query, query]}
        cat = type(self)(
            paths=self.paths,
            query=query,
            handler_registry=self._handler_registry,
            transforms=self._transforms,
            root_map=self._root_map,
            name='search results',
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options)
        return cat

    def _get_serializer(self):
        "This is used internally by v1.Broker. It may be removed in future."
        from suitcase.msgpack import Serializer
        from event_model import RunRouter
        path, *_ = self.paths
        directory = os.path.dirname(path)

        def factory(name, doc):
            serializer = Serializer(directory)
            return [serializer], []

        return RunRouter([factory])
