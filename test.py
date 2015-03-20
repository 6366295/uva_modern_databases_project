from yamr import Database, Chunk, Tree
from mapreduce_wrapper import Script

a = Script()

a.add_file('map.py')

db = Database('test.db', max_size=4)


db[3] = 'foo'
db[5] = 'bar'
db[1] = 'test'
db[7] = 'this'
db[9] = 'that'

db.commit()