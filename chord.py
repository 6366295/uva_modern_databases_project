import math

class Finger(object):
    def __init__(self, start, node):
        self.start = start
        self.node = node

class Node(object):
    ring_size = 2 ** 5
    finger_count = int(math.log(ring_size, 2))
    next_finger = 0

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
        print('Predecessor: {}'.format(self.node_id))

    def join(self, node):
        self.predecessor = None
        self.successor = node.find_successor(self)

    def stabilise(self):
        x = self.successor.predecessor 
        if self.in_range(x.node_id, self.node_id, self.successor.node_id):
            self.successor = x
            
        self.successor.notify(self)

    def notify(self, node):
        if (self.predecessor is None) or self.in_range(node.node_id, self.predecessor.node_id, self.node_id):
            self.predecessor = node

    def fix_fingers(self):
        self.next_finger += 1
        if self.next_finger > self.finger_count:
            self.next_finger = 1
        self.fingers[self.next_finger].node = self.find_successor(self.fingers[self.next_finger].start)

    def find_successor(self, node_id):
        if self.in_range(node_id, self.node_id, self.successor.node_id):
            return self.successor
        else:
            n0 = self.closest_preceding_node(node_id)
            return n0.find_successor(node_id)
    

    def closest_preceding_node(self, node_id):
        for i in reversed((range(1, self.finger_count))):
            if self.in_range(self.fingers[i].node.node_id, self.node_id, node_id):
                return self.fingers[i].node

    def is_alive(self):
        return self.alive

    def kill(self):
        self.alive = False

