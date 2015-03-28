'''
  Name: Hidde Hensel
  Studentnr: 6379176
  
  Name: Mike Trieu
  Studentnr: 6366295 / 10105093
  
  Chord implementation using template code provided by UvA
'''

import math

class Finger(object):
    def __init__(self, start, node):
        self.start = start
        self.node = node

class Node(object):
    ring_size = 2 ** 5
    finger_count = int(math.log(ring_size, 2))
    next_finger = 0
    successor_list = []
    r = 3

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
        print('Predecessor: {}'.format(self.predecessor.node_id))

    
    '''
      join, stabilise, notify, fix_fingers, find_successor,
        closest_preceding_node code are mostly inspired of pseudo code from  
        en.wikipedia.org/wiki/Chord_(peer-to-peer) and the Chord: A scalable
        peer-to-peer lookup service for internet application article
    '''
    # Add current node to the ring
    def join(self, node):
        self.predecessor = None
        self.successor = node.find_successor(self.node_id)

    # Current node ask succesor's predecessor x and decides if it should be it's
    #   successor. Notify succesor that he is new predecessor.
    def stabilise(self):
        # if node is dead, use successor_list to find the next successor
        if not self.successor.predecessor.is_alive():
            for next_successor in self.successor_list:
                if next_successor.is_alive():
                    self.successor = next_successor
                    break
                
        x = self.successor.predecessor 

        if self.in_range(x.node_id, self.node_id+1, self.successor.node_id+1):
            self.successor = x
        
        self.successor_list = []
        temp = self.successor
        for _ in range(self.r):
            temp = temp.successor
            self.successor_list.append(temp)
            
        self.successor.notify(self)

    def notify(self, node):
        if (self.predecessor is None) or \
        self.in_range(node.node_id, self.predecessor.node_id+1, self.node_id):
            self.predecessor = node

    # Update finger tables
    def fix_fingers(self):
        for i in range(1, self.finger_count):
            self.fingers[i].node = self.find_successor(self.node_id+(2**(i-1)))

    # Find successor, return if in range. Else find closest preceding node
    def find_successor(self, node_id):
        if self.in_range(node_id, self.node_id+1, self.successor.node_id+1):
            return self.successor
        else:
            n0 = self.closest_preceding_node(node_id)
            return n0.find_successor(node_id)

    # return closest preceding node
    def closest_preceding_node(self, node_id):
        for i in reversed((range(1, self.finger_count))):
            if self.in_range(self.fingers[i].node.node_id, self.node_id+1,
                             node_id):
                return self.fingers[i].node
        return self

    def is_alive(self):
        return self.alive

    def kill(self):
        self.alive = False

