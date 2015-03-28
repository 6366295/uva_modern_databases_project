import math

class Finger(object):
    def __init__(self, start, node):
        self.start = start
        self.node = node

class Node(object):
    ring_size = 2 ** 5
    finger_count = int(math.log(ring_size, 2))

    @property
    def successor(self):
        return self.fingers[0].node

    @successor.setter
    def successor(self, value):
        self.fingers[0].node = value

    def __init__(self, node_id):
        self.alive = True
        self.node_id = node_id
        self.predecessor = self
        self.fingers = [Finger((node_id + 2 ** k) % self.ring_size, self) for k
            in range(self.finger_count)]

    def distance(self, lhs, rhs):
        return (self.ring_size + rhs - lhs) % self.ring_size

    def in_range(self, value, lower, upper):
        if lower is upper:
            return True

        return self.distance(lower, value) < self.distance(lower, upper)

    def print_fingers(self):
        print('Finger table for node #{}:'.format(self.node_id))
        print('\n'.join('{}: {}'.format(finger.start, finger.node.node_id) for
            finger in self.fingers))

    def join(self, node):
        self.predecessor = None
        self.successor = node.find_successor(self)
        pass

    def stabilise(self):
        x = successor.predecessor 
        if self is not x:
            self.successor = x
        successor.notify(self)


        pass

    def notify(self, node):
        if self.predecessor is self or self.predecessor is not node:
            self.predecessor = node
        pass

    def fix_fingers(self):
        fingersize = self.finger_count
        for i in range(fingersize):
            next = self.node_id + 1 
            if next > self.ring_size - 1:
                next = 0;
            self.fingers[i] = find_successor(self.node_id + 2 ** (next -1))
        pass

    def find_successor(self, node_id):
        #self is known node
        #nieuwe node is node_id
        node = node_id
        node_id = node.node_id
        if self.successor.node_id is node_id:
            return node
        else:
            n0 = self.closest_preceding_node(node_id)
            return n0.find_successor(node)
    

    def closest_preceding_node(self, node_id):
        tempnode = self.fingers[-1]
        for i in reversed(range(len(self.fingers))):
            if self.fingers[i].node.node_id < node_id:
                return tempnode
            tempnode = self.fingers[i].node
        return self

    def is_alive(self):
        return self.alive

    def kill(self):
        self.alive = False

