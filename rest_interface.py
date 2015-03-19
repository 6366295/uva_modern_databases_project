from yamr import Database, Chunk, Tree
from mapreduce_wrapper import Script
import json
import os

import tornado.ioloop
import tornado.web

class DocumentsHandler(tornado.web.RequestHandler):    
    def get(self):
        db = Database('test.db', max_size=4)
        
        for k, v in db.items():
            self.write(str(k) + ': ' + str(v.decode('utf-8')) + '\n')
            
        db.close()
            
    def post(self):
        data = self.request.body
        
        datadecode = data.decode("utf-8")

        db = Database('test.db', max_size=4)
        key = max(db.keys(), key=int) + 1
        value = datadecode
        db[int(key)] = value

        db.commit()

        db.close()
            
    def put(self):
        data = self.request.body
        data_json = json.loads(data.decode("utf-8"))
        
        db = Database('test.db', max_size=4)
        
        for k, v in data_json.items():
            db[int(k)] = v
            
        db.commit()
        
        db.close()
        
class SingleDocumentHandler(tornado.web.RequestHandler):    
    def get(self, doc_id):
        db = Database('test.db', max_size=4)
        
        self.write(str(db[int(doc_id)].decode('utf-8')) + '\n')
            
        db.close()
        
        
    def put(self, doc_id):
        data = self.request.body
        
        db = Database('test.db', max_size=4)
        
        db[int(doc_id)] = data.decode("utf-8")
            
        db.commit()
        
        db.close()
        
class MapHandler(tornado.web.RequestHandler):
    def get(self):        
        os.remove('emit.db')
        
        mapreduce_script = Script()
        
        mapreduce_script.add_file('emit.py')
        mapreduce_script.add_file('map.py')
        mapreduce_script.symtable['emit_dict'] = {}
        
        db = Database('test.db', max_size=4)
        temp_emit_db = Database('emit.db', max_size=4)
        
        for v in db.values():
            mapreduce_script.invoke('dbMap', doc=v)
            
            emit_dict = mapreduce_script.symtable['emit_dict']
            
            for k2, v2 in emit_dict.items():
                if k2 in temp_emit_db:
                    temp_emit_db[k2].extend(v2)
                else:
                    temp_emit_db[k2] = v2
                    
            mapreduce_script.symtable['emit_dict'] = {}
            
        temp_emit_db.commit()
        
        db.close()
        temp_emit_db.close()
        
class ReduceHandler(tornado.web.RequestHandler):
    def get(self):
        os.remove('reduce.db')
        
        mapreduce_script = Script()
        mapreduce_script.add_file('map.py')
        emit_db = Database('emit.db', max_size=4)
        reduce_db = Database('reduce.db', max_size=4)

        for k, v in emit_db.items():
           reduced_value = mapreduce_script.invoke('dbReduce', key=k, values=v)

           reduce_db[k] = str(reduced_value)

        reduce_db.commit()
        
        genexp = ((k, reduce_db[k]) for k in sorted(reduce_db, key=reduce_db.get, reverse=True))
        for k, v in genexp:
            print(k.decode("utf-8") + ' : ' + str(v) + '\n')  

        reduce_db.close()
        emit_db.close()
        
        
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
    (r"/documents/reduce", ReduceHandler),
    (r"/web/documents", WebDocumentsHandler)
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()  
