import tornado.web
import tornado.template

import base64
import os

import settings
from ns import nslib


def require_basic_auth(handler_class):
    def wrap_execute(handler_execute):
        def require_basic_auth(handler, kwargs):
            auth_header = handler.request.headers.get('Authorization')
            if auth_header is None or not auth_header.startswith('Basic '):
                handler.set_status(401)
                handler.set_header('WWW-Authenticate', 'Basic realm=Restricted')
                handler._transforms = []
                handler.finish()
                return False
            auth_decoded = base64.decodestring(auth_header[6:])
            # kwargs['basicauth_user'], kwargs['basicauth_pass'] = auth_decoded.split(':', 2)
            user, pwd = auth_decoded.split(':', 2)
            real_user, real_pwd = file(os.path.join(settings.datadir, 'pwd')).read().strip().split(":")
            return (user == real_user) and (real_pwd == pwd)

        def _execute(self, transforms, *args, **kwargs):
            if not require_basic_auth(self, kwargs):
                return False
            return handler_execute(self, transforms, *args, **kwargs)
        return _execute

    handler_class._execute = wrap_execute(handler_class._execute)
    return handler_class


@require_basic_auth
class StatsUIHandler(tornado.web.RequestHandler):
    def get(self):
        loader = tornado.template.Loader(settings.staticdir)
        s_list, full_info = nslib.scan_stats(False)

        self.write(loader.load("stats.html").generate(storages=s_list, \
                full_info=full_info))
        return


@require_basic_auth
class FSUIHandler(tornado.web.RequestHandler):
    def get(self, path):
        loader = tornado.template.Loader(settings.staticdir)
        self.write(loader.load("fs.html").generate(myvalue="XXX"))
        return
