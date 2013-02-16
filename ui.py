import tornado.web
import tornado.template

import os

from storages import storages
import settings
from ns import nslib


class StatsUIHandler(tornado.web.RequestHandler):
    def get(self):
        loader = tornado.template.Loader(settings.staticdir)
        s_list = []
        for storage in storages.values():
            s_list.append({"name": storage.name(),
                           "size": storage.allow_space / 1024. / 1024 / 1024,
                           "free": 100,  # storage.free(),
                           "chunks_count": 5,  # storage.chunks_count(),
                           })

        self.write(loader.load("stats.html").generate(storages=s_list))
        return


class FSUIHandler(tornado.web.RequestHandler):
    def get(self):
        loader = tornado.template.Loader(settings.staticdir)
        self.write(loader.load("fs.html").generate(myvalue="XXX"))
        return
