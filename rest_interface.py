from yamr import Database, Chunk, Tree
from mapreduce_wrapper import Script
import json

import tornado.ioloop
import tornado.web

class DocumentsHandler(tornado.web.RequestHandler):    
    def get(self):
        db = Database('test.db', max_size=4)
        
        for k, v in db.items():
            self.write('{}: {}'.format(k, v))
            
        db.close()
            
    def post(self):
        data = self.request.body
        jsondata = json.loads(data.decode("utf-8"))
        jsondatalength = len(jsondata)

        db = Database('test.db', max_size=4)
        key = list(jsondata.keys())[jsondatalength-1] 
        value = list(jsondata.values())[jsondatalength-1]
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
        
        self.write(db[int(doc_id)])
            
        db.close()
        
        
    def put(self, doc_id):
        data = self.request.body
        
        db = Database('test.db', max_size=4)
        
        db[int(doc_id)] = data.decode("utf-8")
            
        db.commit()
        
        db.close()
        
class MapreduceHandler(tornado.web.RequestHandler):
    def get(self):        
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
        
application = tornado.web.Application([
    (r"/documents", DocumentsHandler),
    (r"/document/([0-9]+)", SingleDocumentHandler),
    (r"/documents/mapreduce", MapreduceHandler)
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()  
