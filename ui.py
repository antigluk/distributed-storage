import tornado.web
import tornado.template

import os

from storages import storages
import settings
from ns import nslib


class UIHandler(tornado.web.RequestHandler):
    def get(self, path):
        self.write("UI")
        loader = tornado.template.Loader(settings.staticdir)
        self.write(loader.load("fs.html").generate(myvalue="XXX"))
        return
