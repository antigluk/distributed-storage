import tornado.web
import json


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(json.dumps({"info": "Hello from flask."}))
