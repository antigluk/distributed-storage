import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.httputil

import settings


class InternalHandler(tornado.web.RequestHandler):
    def get(self, path):
        if path == "ip":
            self.write(settings.internal_ip)

        if path.startswith("chunk/"):
            hash = path[len("chunk/"):]
            # if os.path.exists():
            self.write(file("/tmp/cache/" + hash).read())
