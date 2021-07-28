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


def key_from_filename(filename):
    "'blah.jsonl' -> 'blah'"
    return Path(filename).stem


class Tree(FileTree):

    # This is set up in Tree.from_directory.
    DEFAULT_READERS_BY_MIMETYPE = {}

    specs = ["CatalogOfBlueskyRuns"]

    @classmethod
    def from_directory(cls, directory):

        tree = MongoNormalizedTree.from_mongomock()
        jsonl_reader = JSONLReader(tree)
        msgpack_reader = MsgpackReader(tree)
        mimetypes_by_file_ext = {
            ".jsonl": "application/x-bluesky-jsonl",
            ".msgpack": "application/x-bluesky-msgpack",
        }
        readers_by_mimetype = {
            "application/x-bluesky-jsonl": jsonl_reader.consume_file,
            "application/x-bluesky-msgpack": msgpack_reader.consume_file,
        }
        return super().from_directory(
            directory,
            readers_by_mimetype=readers_by_mimetype,
            mimetypes_by_file_ext=mimetypes_by_file_ext,
            key_from_filename=key_from_filename,
            tree=tree,
        )

    def __init__(self, *args, tree, **kwargs):
        self._tree = tree
        super().__init__(*args, **kwargs)

    def new_variation(self, **kwargs):
        return super().new_variation(tree=self._tree, **kwargs)

    def search(self, *args, **kwargs):
        return self._tree.search(*args, **kwargs)
