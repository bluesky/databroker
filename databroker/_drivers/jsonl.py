import glob
import json
import os
import pathlib
import event_model

from ..in_memory import BlueskyInMemoryCatalog
from ..core import tail


def gen(filename):
    """
    A JSONL file generator.

    Parameters
    ----------
    filename: str
        JSONL file to load.
    """
    with open(filename, 'r') as file:
        for line in file:
            name, doc = json.loads(line)
            yield (name, doc)


def get_stop(filename):
    """
    Returns the stop_doc of a Bluesky JSONL file.

    The stop_doc is always the last line of the file.

    Parameters
    ----------
    filename: str
        JSONL file to load.
    Returns
    -------
    stop_doc: dict or None
        A Bluesky run_stop document or None if one is not present.
    """
    stop_doc = None
    lastline, = tail(filename)
    if lastline:
        try:
            name, doc = json.loads(lastline)
        except json.JSONDecodeError:
            ...
            # stop_doc will stay None if it can't be decoded correctly.
        else:
            if (name == 'stop'):
                stop_doc = doc
    return stop_doc


class BlueskyJSONLCatalog(BlueskyInMemoryCatalog):
    name = 'bluesky-jsonl-catalog'  # noqa

    def __init__(self, paths, *, handler_registry=None, root_map=None,
                 filler_class=event_model.Filler,
                 query=None, **kwargs):
        """
        This Catalog is backed by a newline-delimited JSON (jsonl) file.

        Each line of the file is expected to be a JSON list with two elements,
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
                         root_map=root_map,
                         filler_class=filler_class,
                         query=query, **kwargs)

    def _load(self):
        for path in self.paths:
            for filename in glob.glob(path):
                mtime = os.path.getmtime(filename)
                if mtime == self._filename_to_mtime.get(filename):
                    # This file has not changed since last time we loaded it.
                    continue
                self._filename_to_mtime[filename] = mtime
                with open(filename, 'r') as file:
                    try:
                        name, start_doc = json.loads(file.readline())
                    except json.JSONDecodeError as e:
                        if not file.readline():
                            # Empty file, maybe being written to currently
                            continue
                        raise e
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
        from suitcase.jsonl import Serializer
        from event_model import RunRouter
        path, *_ = self.paths
        directory = os.path.dirname(path)

        def factory(name, doc):
            serializer = Serializer(directory)
            serializer(name, doc)
            return [serializer], []

        return RunRouter([factory])
