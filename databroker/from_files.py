from tiled.adapters.files import DirectoryAdapter

from .mongo_normalized import Tree as MongoNormalizedTree


class EmptyFile(FileNotFoundError):
    pass


class JSONLAdapter:

    specs = ["BlueskyRun"]

    def __init__(self, in_memory_mongo_tree):
        self._in_memory_mongo_tree = in_memory_mongo_tree

    def consume_file(self, filepath):
        import json

        serializer = self._in_memory_mongo_tree.get_serializer()
        with open(filepath) as file:
            lines = iter(file)
            try:
                name, doc = json.loads(next(lines))
            except StopIteration:
                raise EmptyFile(filepath)
            assert name == "start"
            uid = doc["uid"]
            for line in lines:
                name, doc = json.loads(line)
                serializer(name, doc)
        return self._in_memory_mongo_tree[uid]


class MsgpackAdapter:

    specs = ["BlueskyRun"]

    def __init__(self, in_memory_mongo_tree):
        self._in_memory_mongo_tree = in_memory_mongo_tree

    def consume_file(self, filepath):
        import msgpack

        serializer = self._in_memory_mongo_tree.get_serializer()
        with open(filepath) as file:
            for name, doc in msgpack.Unpacker(file):
                serializer(name, doc)

        return self._in_memory_mongo_tree[self._ser]


class JSONLTree(DirectoryAdapter):

    # This is set up in Tree.from_directory.
    DEFAULT_READERS_BY_MIMETYPE = {}

    specs = ["CatalogOfBlueskyRuns"]

    @classmethod
    def from_directory(cls, directory, *, handler_registry=None):

        in_memory_mongo_tree = MongoNormalizedTree.from_mongomock(
            handler_registry=handler_registry
        )
        jsonl_reader = JSONLAdapter(in_memory_mongo_tree)
        mimetypes_by_file_ext = {
            ".jsonl": "application/x-bluesky-jsonl",
        }
        readers_by_mimetype = {
            "application/x-bluesky-jsonl": jsonl_reader.consume_file,
        }
        return super().from_directory(
            directory,
            readers_by_mimetype=readers_by_mimetype,
            mimetypes_by_file_ext=mimetypes_by_file_ext,
            in_memory_mongo_tree=in_memory_mongo_tree,
        )

    def __init__(self, *args, in_memory_mongo_tree, **kwargs):
        self._in_memory_mongo_tree = in_memory_mongo_tree
        super().__init__(*args, **kwargs)

    def new_variation(self, **kwargs):
        return super().new_variation(
            in_memory_mongo_tree=self._in_memory_mongo_tree, **kwargs
        )

    def search(self, *args, **kwargs):
        return self._in_memory_mongo_tree.search(*args, **kwargs)

    def get_serializer(self):
        import suitcase.jsonl

        file_tree = self
        serializer = self._in_memory_mongo_tree.get_serializer()

        class _Serializer(suitcase.jsonl.Serializer):
            def __call__(self, name, doc):
                super().__call__(name, doc)
                serializer(name, doc)
                if name in {"start", "stop"}:
                    file_tree.update_now(timeout=5)

        return _Serializer(self.directory)


class MsgpackTree(DirectoryAdapter):

    # This is set up in Tree.from_directory.
    DEFAULT_READERS_BY_MIMETYPE = {}

    specs = ["CatalogOfBlueskyRuns"]

    @classmethod
    def from_directory(cls, directory, *, handler_registry=None):

        in_memory_mongo_tree = MongoNormalizedTree.from_mongomock(
            handler_registry=handler_registry
        )
        msgpack_reader = MsgpackAdapter(in_memory_mongo_tree)
        mimetypes_by_file_ext = {
            ".msgpack": "application/x-bluesky-msgpack",
        }
        readers_by_mimetype = {
            "application/x-bluesky-msgpack": msgpack_reader.consume_file,
        }
        return super().from_directory(
            directory,
            readers_by_mimetype=readers_by_mimetype,
            mimetypes_by_file_ext=mimetypes_by_file_ext,
            in_memory_mongo_tree=in_memory_mongo_tree,
        )

    def __init__(self, *args, in_memory_mongo_tree, **kwargs):
        self._in_memory_mongo_tree = in_memory_mongo_tree
        super().__init__(*args, **kwargs)

    def new_variation(self, **kwargs):
        return super().new_variation(
            in_memory_mongo_tree=self._in_memory_mongo_tree, **kwargs
        )

    def search(self, *args, **kwargs):
        return self._in_memory_mongo_tree.search(*args, **kwargs)

    def get_serializer(self):
        import suitcase.msgpack

        tree = self
        serializer = self._in_memory_mongo_tree.get_serializer()

        class _Serializer(suitcase.jsonl.Serializer):
            def __call__(self, name, doc):
                super().__call__(name, doc)
                serializer(name, doc)
                if name in {"start", "stop"}:
                    tree.update_now(timeout=5)

        return _Serializer(self.directory)
