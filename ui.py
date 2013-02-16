import tornado.web
import tornado.template

import settings
from ns import nslib


class StatsUIHandler(tornado.web.RequestHandler):
    def get(self):
        loader = tornado.template.Loader(settings.staticdir)
        s_list, full_info = nslib.scan_stats()

        self.write(loader.load("stats.html").generate(storages=s_list, \
                full_info=full_info))
        return


class FSUIHandler(tornado.web.RequestHandler):
    def get(self, path):
        loader = tornado.template.Loader(settings.staticdir)
        self.write(loader.load("fs.html").generate(myvalue="XXX"))
        return
