'''
  Name: Hidde Hensel
  Studentnr: 6379176
  
  Name: Mike Trieu
  Studentnr: 6366295 / 10105093
  
  REST-interface implementation using python Tornado framework for a document
    store database.
'''

from yamr import Database, Chunk, Tree
from mapreduce_wrapper import Script
import json
import os

import tornado.ioloop
import tornado.web

# Operations on the entire collection of documents
class DocumentsHandler(tornado.web.RequestHandler):    
    # GET: Retrieves list of all key, value pairs in the database
    def get(self):
        db = Database('test.db', max_size=4)
        
        for k, v in db.items():
            self.write(str(k) + ': ' + str(v.decode('utf-8')) + '\n')
            
        db.close()
            
    # POST: Creates new document
    def post(self):
        data = self.request.body
        value = data.decode("utf-8")

        db = Database('test.db', max_size=4)
        
        # Get largest key in database + 1, because this one should always be
        #   empty
        key = max(db.keys(), key=int) + 1
        
        db[int(key)] = value

        db.commit()
        
        self.write('Document inserted in key ' + str(key) + '\n')

        db.close()
            
    # PUT: Replace entire collection of documents with another
    def put(self):
        # Delete database, since the entire collection will be replaced
        os.remove('test.db')
        
        data = self.request.body
        data_json = json.loads(data.decode("utf-8"))
        
        db = Database('test.db', max_size=4)
        
        for k, v in data_json.items():
            db[int(k)] = v
            
        db.commit()
        
        db.close()

# Operations on elements
class SingleDocumentHandler(tornado.web.RequestHandler):    
    # GET: Retrieve a document
    def get(self, doc_id):
        db = Database('test.db', max_size=4)
        
        self.write(str(db[int(doc_id)].decode('utf-8')) + '\n')
            
        db.close()
        
    # PUT: Update existing document
    def put(self, doc_id):
        # data get a string from body
        data = self.request.body
        decoded_data = data.decode("utf-8")
        
        db = Database('test.db', max_size=4)
        
        # Update document, if it exists
        if int(doc_id) in db:
            db[int(doc_id)] = decoded_data
            self.write('Updated document in key value: ' +\
                str(doc_id) + '\n')
        else:
            self.write('Document does not exist!' + '\n')
            
        db.commit()
        
        db.close()
        
# Applies map query to all documents
class MapHandler(tornado.web.RequestHandler):
    def post(self):
        os.remove('emit.db')
        
        mapreduce_script = Script()
        
        mapreduce_script.add_file('emit.py')
        mapreduce_script.add_file('map.py')
        mapreduce_script.symtable['emit_dict'] = {}
        
        # Store temporary results in emit.db
        db = Database('test.db', max_size=4)
        temp_emit_db = Database('emit.db', max_size=4)
        
        # Forward each document to the map query
        for v in db.values():
            mapreduce_script.invoke('dbMap', doc=v)
            
            # Every time emit function is called in the map query, a key and
            #   value is stored in emit_dict
            emit_dict = mapreduce_script.symtable['emit_dict']
            
            # Store in temporary database and group values with keys
            for k2, v2 in emit_dict.items():
                if k2 in temp_emit_db:
                    temp_emit_db[k2].extend(v2)
                else:
                    temp_emit_db[k2] = v2
                    
            # Empty emit_dict for next document
            mapreduce_script.symtable['emit_dict'] = {}
            
        temp_emit_db.commit()
        
        db.close()
        temp_emit_db.close()
        
# Show map query result
class MapResultHandler(tornado.web.RequestHandler):
    def get(self):
        emit_db = Database('emit.db', max_size=4)
        
        for k, v in emit_db.items():
            self.write(str(k) + ': ' + str(v) + '\n')
            
        emit_db.close()
        
# Applies reduce query to result of map querie
class ReduceHandler(tornado.web.RequestHandler):
    def post(self):
        os.remove('reduce.db')
        
        mapreduce_script = Script()
        mapreduce_script.add_file('map.py')
        
        # Use temporary reduce.db to store the reduced results
        emit_db = Database('emit.db', max_size=4)
        reduce_db = Database('reduce.db', max_size=4)

        # Forward every key value pair to the map query
        for k, v in emit_db.items():
            reduced_value = mapreduce_script.invoke('dbReduce', key=k, values=v)

            # Store key and reduce value in reduce.db
            reduce_db[k] = str(reduced_value)

        reduce_db.commit()
             
        reduce_db.close()
        emit_db.close()
        
# Show reduce query result
class ReduceResultHandler(tornado.web.RequestHandler):
    def get(self):
        reduce_db = Database('reduce.db', max_size=4)
        
        genexp = ((k, reduce_db[k]) for k in sorted(reduce_db,
                                                    key=reduce_db.get,
                                                    reverse=True))
        
        for k, v in genexp:
            self.write(k.decode("utf-8") + ' : ' +\
                str(v.decode("utf-8")) + '\n') 
            
        reduce_db.close()

# Compact the database
class CompactionHandler(tornado.web.RequestHandler):    
    def get(self):
        db = Database('test.db', max_size=4)
        
        db.compaction()
            
        db.close()
  
class WebDocumentsHandler(tornado.web.RequestHandler):    
    # Override function to find html files in web_interface folder
    def get_template_path(self):
        return 'web_interface'
    
    def get(self):
        db = Database('test.db', max_size=4)
        
        self.render("documents.html", title="All documents in database", db=db)
            
        db.close()
        
application = tornado.web.Application([
    (r"/documents", DocumentsHandler),
    (r"/document/([0-9]+)", SingleDocumentHandler),
    (r"/documents/map", MapHandler),
    (r"/documents/map/result", MapResultHandler),
    (r"/documents/reduce", ReduceHandler),
    (r"/documents/reduce/result", ReduceResultHandler),
    (r"/documents/compaction", CompactionHandler),
    (r"/web/documents", WebDocumentsHandler)
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()  
