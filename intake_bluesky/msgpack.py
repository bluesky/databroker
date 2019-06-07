import glob
import msgpack
import msgpack_numpy
import os
import pathlib

from .in_memory import BlueskyInMemoryCatalog


UNPACK_OPTIONS = dict(object_hook=msgpack_numpy.decode,
                      raw=False,
                      max_buffer_size=1_000_000_000)


class BlueskyMsgpackCatalog(BlueskyInMemoryCatalog):
    name = 'bluesky-msgpack-catalog'  # noqa

    def __init__(self, paths, *,
                 handler_registry=None, query=None, **kwargs):
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
            Maps each asset spec to a handler class or a string specifying the
            module name and class name, as in (for example)
            ``{'SOME_SPEC': 'module.submodule.class_name'}``.
        query : dict, optional
            Mongo query that filters entries' RunStart documents
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
                         query=query,
                         **kwargs)

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
                        name, run_start_doc = next(unpacker)
                    except StopIteration:
                        # Empty file, maybe being written to currently
                        continue

                def gen():
                    with open(filename, 'rb') as file:
                        yield from msgpack.Unpacker(file, **UNPACK_OPTIONS)
                self.upsert(gen, (), {})

    def search(self, query):
        """
        Return a new Catalog with a subset of the entries in this Catalog.

        Parameters
        ----------
        query : dict
        """
        if self._query:
            query = {'$and': [self._query, query]}
        cat = type(self)(
            paths=self.paths,
            query=query,
            handler_registry=self.filler.handler_registry,
            name='search results',
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options)
        return cat
