"""
Author: S.J.R. van Schaik <stephan@synkhronix.com>

Reference implementation of the database abstraction for yet another map-reduce
database (yamr-db).
"""

from collections import MutableMapping
from msgpack import packb, unpackb

from .btree import LazyNode, Tree
from .chunk import Chunk, ChunkId

class Database(MutableMapping):
    def __init__(self, path, max_size=1024):
        self.chunk = Chunk(open(path, 'ab+'))
        
        offset = None
        size = 0
        chunk = Chunk(open(path, 'rb+'))

        try:
            while chunk.verify():
                if chunk.get_id() == ChunkId.Commit:
                    offset = chunk.tell()
                    print(chunk.get_size())
                    chunk.next()
                    size = chunk.tell()
                else:
                    chunk.next()
        except EOFError:
            pass

        print(offset)
        print(size)

        chunk.f.truncate(size)

        if offset is None:
            self.tree = Tree(self.chunk, max_size)
        else:
            chunk.seek(offset)
            data = unpackb(chunk.read())

            self.tree = Tree(self.chunk, data[b'max_size'])
            self.tree.root = LazyNode(offset=data[b'documents'], tree=self.tree)

    def commit(self):
        self.tree.commit()

        data = packb({
            'documents': self.tree.root.offset,
            'max_size': self.tree.max_size,
        })

        self.tree.chunk.write(ChunkId.Commit, data)

    def __getitem__(self, key):
        return self.tree[key]

    def __setitem__(self, key, value):
        self.tree[key] = value

    def __delitem__(self, key):
        del self.tree[key]

    def __len__(self):
        return len(self.tree)

    def __iter__(self):
        yield from self.tree

