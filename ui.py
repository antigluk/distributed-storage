import tornado.web
import tornado.template

import os

from storages import storages
import settings
from ns import nslib


class StatsUIHandler(tornado.web.RequestHandler):
    def get(self):
        loader = tornado.template.Loader(settings.staticdir)
        self.write(loader.load("stats.html").generate(myvalue="XXX"))
        return


class FSUIHandler(tornado.web.RequestHandler):
    def get(self):
        loader = tornado.template.Loader(settings.staticdir)
        self.write(loader.load("fs.html").generate(myvalue="XXX"))
        return
