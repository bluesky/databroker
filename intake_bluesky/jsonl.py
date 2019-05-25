from .in_memory import BlueskyInMemoryCatalog
import glob
import json
import pathlib


class BlueskyJSONLCatalog(BlueskyInMemoryCatalog):
    name = 'bluesky-jsonl-catalog'  # noqa

    def __init__(self, paths, *,
                 handler_registry=None, query=None, **kwargs):
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
            Maps each asset spec to a handler class or a string specifying the
            module name and class name, as in (for example)
            ``{'SOME_SPEC': 'module.submodule.class_name'}``.
        **kwargs :
            Additional keyword arguments are passed through to the base class,
            Catalog.
        """
        # Tolerate a single path (as opposed to a list).
        if isinstance(paths, (str, pathlib.Path)):
            paths = [paths]
        self.paths = paths
        super().__init__(handler_registry=handler_registry,
                         query=query,
                         **kwargs)

    def _load(self):
        # TODO Cache filepaths already loaded and check mtime to decide which
        # need to be reloaded.
        for path in self.paths:
            file_list = glob.glob(path)
            for filename in file_list:
                with open(filename, 'r') as file:
                    try:
                        name, run_start_doc = json.loads(file.readline())
                    except json.JSONDecodeError:
                        if not file.readline():
                            # Empty file, maybe being written to currently
                            continue

                def gen():
                    with open(filename) as file:
                        for line in file:
                            name, doc = json.loads(line)
                            yield (name, doc)
                self.upsert(gen, (), {})
