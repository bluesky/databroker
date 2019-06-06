import copy
import event_model
import intake
import intake.catalog
import intake.catalog.local
import intake.source.base
from mongoquery import Query

from .core import parse_handler_registry


class SafeLocalCatalogEntry(intake.catalog.local.LocalCatalogEntry):
    # For compat with intake 0.5.1.
    # Not necessary after https://github.com/intake/intake/pull/362
    # is released.
    def describe(self):
        return copy.deepcopy(super().describe())


class BlueskyInMemoryCatalog(intake.catalog.Catalog):
    name = 'bluesky-run-catalog'  # noqa

    def __init__(self, handler_registry=None, query=None, **kwargs):
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
            ``{'SOME_SPEC': 'module.submodule.class_name'}``.
        query : dict, optional
            Mongo query that filters entries' RunStart documents
        **kwargs :
            Additional keyword arguments are passed through to the base class,
            Catalog.
        """
        self._query = query or {}
        if handler_registry is None:
            handler_registry = {}
        parsed_handler_registry = parse_handler_registry(handler_registry)
        self.filler = event_model.Filler(parsed_handler_registry)
        self._uid_to_run_start_doc = {}
        super().__init__(**kwargs)

    def upsert(self, gen_func, gen_args, gen_kwargs):
        gen = gen_func(*gen_args, **gen_kwargs)
        name, run_start_doc = next(gen)

        if name != 'start':
            raise ValueError("Expected a generator of (name, doc) pairs where "
                             "the first entry was ('start', {...}).")

        if not Query(self._query).match(run_start_doc):
            return

        uid = run_start_doc['uid']
        self._uid_to_run_start_doc[uid] = run_start_doc

        entry = SafeLocalCatalogEntry(
            name=run_start_doc['uid'],
            description={},  # TODO
            driver='intake_bluesky.core.BlueskyRunFromGenerator',
            direct_access='forbid',
            args={'gen_func': gen_func,
                  'gen_args': gen_args,
                  'gen_kwargs': gen_kwargs,
                  'filler': self.filler},
            cache=None,  # ???
            parameters=[],
            metadata={},  # TODO
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
        return self._entries[uid]

    def __len__(self):
        return len(self._uid_to_run_start_doc)
