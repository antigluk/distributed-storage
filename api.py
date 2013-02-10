import tornado.web

import os
import urllib2


class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        address = os.environ['OPENSHIFT_INTERNAL_IP']
        string = urllib2.urlopen("http://%s:15001/" % address)
        self.write(string)
        self.finish()
