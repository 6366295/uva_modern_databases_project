from yamr import Database, Chunk, Tree

import tornado.ioloop
import tornado.web

class DocumentsHandler(tornado.web.RequestHandler):
    def get(self):
        db = Database('test.db', max_size=4)
        
        for k, v in db.items():
            self.write('{}: {}'.format(k, v))
            
        db.close()
            
    def put(self):
        return
        
application = tornado.web.Application([
    (r"/documents", DocumentsHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start() 
