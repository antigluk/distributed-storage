import tornado.web

import os
import urllib2


class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        address = os.environ['OPENSHIFT_INTERNAL_IP']
        print "Loading..."
        string = urllib2.urlopen("http://%s:15001/" % address)
        print string
        self.write(unicode(string))
        self.finish()
