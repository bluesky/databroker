import event_model
from functools import partial
import glob
import intake
import intake.catalog
import intake.catalog.local
import intake.source.base
import json
import pathlib
from mongoquery import Query

from .core import parse_handler_registry


class BlueskyJSONLCatalog(intake.catalog.Catalog):
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
        name = 'bluesky-jsonl-catalog'  # noqa

        # Tolerate a single path (as opposed to a list).
        if isinstance(paths, (str, pathlib.Path)):
            paths = [paths]
        self.paths = paths
        self._runs = {}  # This maps run_start_uids to file paths.
        self._run_starts = {}  # This maps run_start_uids to run_start_docs.

        self._query = query or {}
        if handler_registry is None:
            handler_registry = {}
        parsed_handler_registry = parse_handler_registry(handler_registry)
        self.filler = event_model.Filler(parsed_handler_registry)
        super().__init__(**kwargs)

    def _load(self):
        for path in self.paths:
            file_list = glob.glob(path)
            for run_file in file_list:
                with open(run_file, 'r') as f:
                    name, run_start_doc = json.loads(f.readline())

                    if name != 'start':
                        raise ValueError(
                            f"Invalid file {run_file}: "
                            f"first line must be a valid start document.")

                    if Query(self._query).match(run_start_doc):
                        run_start_uid = run_start_doc['uid']
                        self._runs[run_start_uid] = run_file
                        self._run_starts[run_start_uid] = run_start_doc

    def _get_run_stop(self, run_start_uid):
        with open(self._runs[run_start_uid], 'r') as run_file:
            name, doc = json.loads(run_file.readlines()[-1])
        if name == 'stop':
            return doc
        else:
            return None

    def _get_event_descriptors(self, run_start_uid):
        descriptors = []
        with open(self._runs[run_start_uid], 'r') as run_file:
            for line in run_file:
                name, doc = json.loads(line)
                if name == 'descriptor':
                    descriptors.append(doc)
        return descriptors

    def _get_event_cursor(self, run_start_uid, descriptor_uids, skip=0, limit=None):
        skip_counter = 0
        descriptor_set = set(descriptor_uids)
        with open(self._runs[run_start_uid], 'r') as run_file:
            events = []
            for line in run_file:
                name, doc = json.loads(line)
                if name == 'event_page' and doc['descriptor'] in descriptor_set:
                    events.extend(event_model.unpack_event_page(doc))
                elif name == 'event' and doc['descriptor'] in descriptor_set:
                    events.append(doc)
                for event in events:
                    if skip_counter >= skip and (limit is None or skip_counter < limit):
                        yield event
                    skip_counter += 1
                    if limit is not None and skip_counter >= limit:
                        break
                events.clear()

    def _get_event_count(self, run_start_uid, descriptor_uids):
        event_count = 0
        descriptor_set = set(descriptor_uids)
        with open(self._runs[run_start_uid], 'r') as run_file:
            for line in run_file:
                name, doc = json.loads(line)
                if name == 'event' and doc['descriptor'] in descriptor_set:
                    event_count += 1
        return event_count

    def _get_resource(self, run_start_uid, uid):
        with open(self._runs[run_start_uid], 'r') as run_file:
            for line in run_file:
                name, doc = json.loads(line)
                if name == 'resource' and doc['uid'] == uid:
                    return doc
        raise ValueError(f"Resource uid {uid} not found.")

    def _get_datum(self, run_start_uid, datum_id):
        with open(self._runs[run_start_uid], 'r') as run_file:
            for line in run_file:
                name, doc = json.loads(line)
                if name == 'datum' and doc['datum_id'] == datum_id:
                    return doc
        raise ValueError(f"Datum_id {datum_id} not found.")

    def _get_datum_cursor(self, run_start_uid, resource_uid, skip=0, limit=None):
        skip_counter = 0
        with open(self._runs[run_start_uid], 'r') as run_file:
            datums = []
            for line in run_file:
                name, doc = json.loads(line)
                if name == 'datum_page' and doc['resource'] == resource_uid:
                    datums.extend(event_model.unpack_datum_page(doc))
                elif name == 'datum' and doc['resource'] == resource_uid:
                    datums.append(doc)
                for datum in datums:
                    if skip_counter >= skip and (limit is None or skip_counter < limit):
                        yield datum
                    skip_counter += 1
                    if limit is not None and skip_counter >= limit:
                        return
                datums.clear()

    def _make_entries_container(self):
        catalog = self

        class Entries:
            "Mock the dict interface around a MongoDB query result."
            def _doc_to_entry(self, run_start_doc):
                uid = run_start_doc['uid']
                entry_metadata = {'start': run_start_doc,
                                  'stop': catalog._get_run_stop(uid)}
                args = dict(
                    get_run_start=lambda: run_start_doc,
                    get_run_stop=partial(catalog._get_run_stop, uid),
                    get_event_descriptors=partial(catalog._get_event_descriptors, uid),
                    get_event_cursor=partial(catalog._get_event_cursor, uid),
                    get_event_count=partial(catalog._get_event_count, uid),
                    get_resource=partial(catalog._get_resource, uid),
                    get_datum=partial(catalog._get_datum, uid),
                    get_datum_cursor=partial(catalog._get_datum_cursor, uid),
                    filler=catalog.filler)
                return intake.catalog.local.LocalCatalogEntry(
                    name=run_start_doc['uid'],
                    description={},  # TODO
                    driver='intake_bluesky.core.RunCatalog',
                    direct_access='forbid',  # ???
                    args=args,
                    cache=None,  # ???
                    parameters=[],
                    metadata=entry_metadata,
                    catalog_dir=None,
                    getenv=True,
                    getshell=True,
                    catalog=catalog)

            def __iter__(self):
                yield from self.keys()

            def keys(self):
                yield from catalog._runs

            def values(self):
                for run_start_doc in catalog._run_starts.values():
                    yield self._doc_to_entry(run_start_doc)

            def items(self):
                for uid, run_start_doc in catalog._run_starts.items():
                    yield uid, self._doc_to_entry(run_start_doc)

            def __getitem__(self, name):
                # If this came from a client, we might be getting '-1'.
                try:
                    N = int(name)
                except (ValueError, TypeError):
                    try:
                        run_start_doc = catalog._run_starts[name]
                    except KeyError:
                        # Try looking up by *partial* uid.
                        matches = {}
                        for uid, run_start_doc in catalog._run_starts.items():
                            if uid.startswith(name):
                                matches[uid] = run_start_doc
                        if not matches:
                            raise KeyError(name)
                        elif len(matches) > 1:
                            match_list = '\n'.join(matches)
                            raise ValueError(
                                f"Multiple matches to partial uid {name!r}:\n"
                                f"{match_list}")
                        else:
                            run_start_doc, = matches.values()
                else:
                    # Sort in reverse chronological order (most recent first).
                    time_sorted = sorted(catalog._run_starts.values(),
                                         key=lambda doc: -doc['time'])
                    if N < 0:
                        # Interpret negative N as "the Nth from last entry".
                        if -N > len(time_sorted):
                            raise IndexError(
                                f"Catalog only contains {len(time_sorted)} "
                                f"runs.")
                        run_start_doc = time_sorted[-N - 1]
                    else:
                        # Interpret positive N as
                        # "most recent entry with scan_id == N".
                        for run_start_doc in time_sorted:
                            if run_start_doc.get('scan_id') == N:
                                break
                        else:
                            raise KeyError(f"No run with scan_id={N}")
                return self._doc_to_entry(run_start_doc)

            def __contains__(self, key):
                # Avoid iterating through all entries.
                try:
                    self[key]
                except KeyError:
                    return False
                else:
                    return True

        return Entries()

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
            paths=list(self._runs.values()),
            query=query,
            name='search results',
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options)
        return cat

    def __len__(self):
        return len(self._runs)
