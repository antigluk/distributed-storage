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
            used, chunk_num = nslib.used_size_on_storage(storage.identifer)
            s_list.append({"name": storage.identifer,
                           "size": "%.2f" % (float(storage.allow_space)),
                           "used": "%.2f" % (float(used) / 1024 / 1024),
                           "free": "%.2f" % ((float(storage.allow_space) - float(used) / 1024 / 1024)),
                           "chunks_count": chunk_num,  # storage.chunks_count(),
                           })

        size, used, count, fs_items = nslib.full_info()
        full_info = {"size": "%.2f" % (float(size) / 1024 / 1024),
                     "used": "%.2f" % (float(used) / 1024 / 1024),
                     "count": count,
                     "free": "%.2f" % (float(size - used) / 1024 / 1024),
                     "fs_items": fs_items,
                     }

        self.write(loader.load("stats.html").generate(storages=s_list, \
                full_info=full_info))
        return


class FSUIHandler(tornado.web.RequestHandler):
    def get(self):
        loader = tornado.template.Loader(settings.staticdir)
        self.write(loader.load("fs.html").generate(myvalue="XXX"))
        return
