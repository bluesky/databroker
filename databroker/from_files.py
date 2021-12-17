from pathlib import Path

from tiled.trees.files import Tree as FileTree
from suitcase.mongo_normalized import Serializer

from .mongo_normalized import Tree as MongoNormalizedTree


class JSONLReader:

    specs = ["BlueskyRun"]

    def __init__(self, tree):
        self._tree = tree
        database = tree.database
        self._serializer = Serializer(database, database)

    def consume_file(self, filepath):
        import json

        with open(filepath) as file:
            lines = iter(file)
            name, doc = json.loads(next(lines))
            assert name == "start"
            uid = doc["uid"]
            self._serializer(name, doc)
            for line in lines:
                name, doc = json.loads(line)
                self._serializer(name, doc)
        return self._tree[uid]


class MsgpackReader:

    specs = ["BlueskyRun"]

    def __init__(self, tree):
        self._tree = tree
        database = tree.database
        self._serializer = Serializer(database, database)

    def consume_file(self, filepath):
        import msgpack

        with open(filepath) as file:
            for name, doc in msgpack.Unpacker(file):
                self._serializer(name, doc)

        return self._tree[self._ser]


class TreeJSONL(FileTree):

    # This is set up in Tree.from_directory.
    DEFAULT_READERS_BY_MIMETYPE = {}

    specs = ["CatalogOfBlueskyRuns"]

    @classmethod
    def from_directory(cls, directory, *, handler_registry=None):

        tree = MongoNormalizedTree.from_mongomock(handler_registry=handler_registry)
        jsonl_reader = JSONLReader(tree)
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
            tree=tree,
        )

    def __init__(self, *args, tree, **kwargs):
        self._tree = tree
        super().__init__(*args, **kwargs)

    def new_variation(self, **kwargs):
        return super().new_variation(tree=self._tree, **kwargs)

    def search(self, *args, **kwargs):
        return self._tree.search(*args, **kwargs)

    def get_serializer(self):
        import suitcase.jsonl

        return suitcase.jsonl.Serializer(self.directory)


class TreeMsgpack(FileTree):

    # This is set up in Tree.from_directory.
    DEFAULT_READERS_BY_MIMETYPE = {}

    specs = ["CatalogOfBlueskyRuns"]

    @classmethod
    def from_directory(cls, directory, *, handler_registry=None):

        tree = MongoNormalizedTree.from_mongomock(handler_registry=handler_registry)
        msgpack_reader = MsgpackReader(tree)
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
            tree=tree,
        )

    def __init__(self, *args, tree, **kwargs):
        self._tree = tree
        super().__init__(*args, **kwargs)

    @property
    def database(self):
        return self._tree.database

    def new_variation(self, **kwargs):
        return super().new_variation(tree=self._tree, **kwargs)

    def search(self, *args, **kwargs):
        return self._tree.search(*args, **kwargs)

    def get_serializer(self):
        import suitcase.msgpack

        return suitcase.msgpack.Serializer(self.directory)
