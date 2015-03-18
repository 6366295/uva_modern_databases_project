from yamr import Database, Chunk, Tree
from mapreduce_wrapper import Script

a = Script()

a.add_file('map.py')

db = Database('emit.db', max_size=4)