from collections import Mapping, MutableMapping
from sortedcontainers import SortedDict

class Tree(MutableMapping):
    def __init__(self, max_size=1024):
        self.root = self._create_leaf(tree=self)
        self.max_size = max_size

    @staticmethod
    def _create_leaf(*args, **kwargs):
        return Leaf(*args, **kwargs)

    @staticmethod
    def _create_node(*args, **kwargs):
        return Node(*args, **kwargs)

    def _create_root(self, lhs, rhs):
        root = self._create_node(tree=self)
        root.rest = lhs
        root.bucket[min(rhs.bucket)] = rhs
        
        return root

    def __getitem__(self, key):
        pass

    def __setitem__(self, key, value):
        """
        Inserts the key and value into the root node. If the node has been
        split, creates a new root node with pointers to this node and the new
        node that resulted from splitting.
        """
        new_node = self.root._insert(key, value)
        
        if new_node != None:
            new_root = self._create_root(self.root, new_node)
            self.root = new_root
            print('split!')

    def __delitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass

class BaseNode(object):
    def __init__(self, tree):
        self.tree = tree
        self.bucket = SortedDict()

    def _split(self):
        """
        Creates a new node of the same type and splits the contents of the
        bucket into two parts of an equal size. The lower keys are being stored
        in the bucket of the current node. The higher keys are being stored in
        the bucket of the new node. Afterwards, the new node is being returned.
        """
        other = self.__class__(tree=self)
        size = int(len(self.bucket) / 2.0)
        
        for key in reversed(self.bucket):
            other.bucket[key] = self.bucket[key]
            del self.bucket[key]
            
            if len(self.bucket) <= size:
                break

        return other

    def _insert(self, key, value):
        """
        Inserts the key and value into the bucket. If the bucket has become too
        large, the node will be split into two nodes.
        """
        self.bucket[key] = value
        
        if len(self.bucket) > 4:
            return self._split()
        
        return None

class Node(BaseNode):
    def __init__(self, *args, **kwargs):
        self.rest = None

        super(Node, self).__init__(*args, **kwargs)

    def _select(self, key):
        """
        Selects the bucket the key should belong to.
        """
        pass

    def _insert(self, key, value):
        """
        Recursively inserts the key and value by selecting the bucket the key
        should belong to, and inserting the key and value into that back. If the
        node has been split, it inserts the key of the newly created node into
        the bucket of this node.
        """
        pass

class Leaf(Mapping, BaseNode):
    def __getitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass

class LazyNode(object):
    _init = False

    def __init__(self, offset=None, node=None):
        """
        Sets up a proxy wrapper for a node at a certain disk offset.
        """
        self.offset = offset
        self.node = node
        self._init = True

    @property
    def changed(self):
        """
        Checks if the node has been changed.
        """
        if self.node is None:
            return False

        return self.node.changed

    def _commit(self):
        """
        Commit the changes if the node has been changed.
        """
        if not self.changed:
            return

        self.node._commit()
        self.changed = False

    def _load(self):
        """
        Load the node from disk.
        """
        pass

    def __getattr__(self, name):
        """
        Loads the node if it hasn't been loaded yet, and dispatches the request
        to the node.
        """
        if not self.node:
            self.node = self._load()
        
        return getattr(self.node, name)

    def __setattr__(self, name, value):
        """
        Dispatches the request to the node, if the proxy wrapper has been fully
        set up.
        """
        if not self._init or hasattr(self, name):
            return super().__setattr__(name, value)

        setattr(self.node, name, value)

newBPTree = Tree()
newBPTree.__setitem__(1,5)
print(newBPTree.root.bucket)
newBPTree.__setitem__(2,5)
print(newBPTree.root.bucket)
newBPTree.__setitem__(3,5)
print(newBPTree.root.bucket)
newBPTree.__setitem__(4,5)
print(newBPTree.root.bucket)
newBPTree.__setitem__(5,5)
print(newBPTree.root.bucket)
print(newBPTree.root.rest.bucket)
print(newBPTree.root.bucket[3].bucket)