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
        pass

    def stabilise(self):
        pass

    def notify(self, node):
        pass

    def fix_fingers(self):
        pass

    def find_successor(self, node_id):
        pass

    def closest_preceding_node(self, node_id):
        pass

    def is_alive(self):
        return self.alive

    def kill(self):
        self.alive = False

