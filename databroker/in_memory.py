import copy
import event_model
import intake
import intake.catalog
import intake.catalog.local
import intake.source.base
from mongoquery import Query


from .core import parse_handler_registry, discover_handlers, Entry
from .v2 import Broker


class BlueskyInMemoryCatalog(Broker):
    name = 'bluesky-run-catalog'  # noqa

    def __init__(self, handler_registry=None, root_map=None, query=None,
                 **kwargs):
        """
        This Catalog is backed by Python collections in memory.

        Subclasses should define a ``_load`` method (same as any intake
        Catalog) that calls this class's ``upsert`` method (which is particular
        to this class).

        Parameters
        ----------
        handler_registry : dict, optional
            Maps each asset spec to a handler class or a string specifying the
            module name and class name, as in (for example)
            ``{'SOME_SPEC': 'module.submodule.class_name'}``. If None, the
            result of ``databroker.core.discover_handlers()`` is used.
        root_map : dict, optional
            Maps resource root paths to different paths.
        query : dict, optional
            Mongo query that filters entries' RunStart documents
        **kwargs :
            Additional keyword arguments are passed through to the base class,
            Catalog.
        """
        self._query = query or {}
        if handler_registry is None:
            handler_registry = discover_handlers()
        parsed_handler_registry = parse_handler_registry(handler_registry)
        self.filler = event_model.Filler(
            parsed_handler_registry, root_map=root_map, inplace=True)
        self._uid_to_run_start_doc = {}
        super().__init__(**kwargs)

    def upsert(self, start_doc, stop_doc, gen_func, gen_args, gen_kwargs):
        if not Query(self._query).match(start_doc):
            return

        uid = start_doc['uid']
        self._uid_to_run_start_doc[uid] = start_doc

        entry = Entry(
            name=start_doc['uid'],
            description={},  # TODO
            driver='databroker.core.BlueskyRunFromGenerator',
            direct_access='forbid',
            args={'gen_func': gen_func,
                  'gen_args': gen_args,
                  'gen_kwargs': gen_kwargs,
                  'filler': self.filler},
            cache=None,  # ???
            parameters=[],
            metadata={'start': start_doc, 'stop': stop_doc},
            catalog_dir=None,
            getenv=True,
            getshell=True,
            catalog=self)
        self._entries[uid] = entry

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
            query=query,
            handler_registry=self.filler.handler_registry,
            root_map=self.filler.root_map,
            name='search results',
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options)
        for key, entry in self._entries.items():
            args = entry._captured_init_kwargs['args']
            cat.upsert(args['gen_func'],
                       args['gen_args'],
                       args['gen_kwargs'])
        return cat

    def __getitem__(self, name):
        # If this came from a client, we might be getting '-1'.
        try:
            N = int(name)
        except (ValueError, TypeError):
            if name in self._uid_to_run_start_doc:
                uid = name
            else:
                # Try looking up by *partial* uid.
                matches = []
                for uid, run_start_doc in list(self._uid_to_run_start_doc.items()):
                    if uid.startswith(name):
                        matches.append(uid)
                if not matches:
                    raise KeyError(name)
                elif len(matches) > 1:
                    match_list = '\n'.join(matches)
                    raise ValueError(
                        f"Multiple matches to partial uid {name!r}:\n"
                        f"{match_list}")
                else:
                    uid, = matches
        else:
            # Sort in reverse chronological order (most recent first).
            time_sorted = sorted(self._uid_to_run_start_doc.values(),
                                 key=lambda doc: -doc['time'])
            if N < 0:
                # Interpret negative N as "the Nth from last entry".
                if -N > len(time_sorted):
                    raise IndexError(
                        f"Catalog only contains {len(time_sorted)} "
                        f"runs.")
                uid = time_sorted[-N - 1]['uid']
            else:
                # Interpret positive N as
                # "most recent entry with scan_id == N".
                for run_start_doc in time_sorted:
                    if run_start_doc.get('scan_id') == N:
                        uid = run_start_doc['uid']
                        break
                else:
                    raise KeyError(f"No run with scan_id={N}")
        entry = self._entries[uid]
        # The user has requested one specific Entry. In order to give them a
        # more useful object, 'get' the Entry for them. Note that if they are
        # expecting an Entry and try to call ``()`` or ``.get()``, that will
        # still work because BlueskyRun supports those methods and will just
        # return itself.
        return entry.get()  # an instance of BlueskyRun

    def __len__(self):
        return len(self._uid_to_run_start_doc)
