import tornado.web
import json


class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.write(json.dumps({"info": "Hello from flask."}))
        self.finish()
