import event_model

from .core import Entry, retry
from .v2 import Broker
from mongoquery import Query


class BlueskyInMemoryCatalog(Broker):
    name = 'bluesky-run-catalog'  # noqa

    def __init__(self, *, handler_registry=None, root_map=None,
                 filler_class=event_model.Filler, query=None,
                 transforms=None, **kwargs):
        """
        This Catalog is backed by Python collections in memory.

        Subclasses should define a ``_load`` method (same as any intake
        Catalog) that calls this class's ``upsert`` method (which is particular
        to this class).

        Parameters
        ----------
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
        self._query = query or {}
        self._uid_to_run_start_doc = {}

        super().__init__(handler_registry=handler_registry,
                         root_map=root_map, filler_class=filler_class,
                         transforms=transforms, **kwargs)

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
                  'get_filler': self._get_filler,
                  'transforms': self._transforms},
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
            handler_registry=self._handler_registry,
            transforms=self._transforms,
            root_map=self._root_map,
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

    @retry
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
                # Force a reload because a stale cache is a big problem for
                # recency-based lookups.
                self.force_reload()
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
