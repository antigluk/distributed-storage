import tornado.web
from tornado.web import asynchronous

import os
import urllib2
import md5
import sh


def stream_body(cls):
    """
    Authored by http://nephics.com/
    https://github.com/nephics/tornado
    """
    class StreamBody(cls):
        def __init__(self, *args, **kwargs):
            if args[0]._wsgi:
                raise Exception("@stream_body is not supported for WSGI apps")
            self._read_body = False
            if hasattr(cls, 'post'):
                cls.post = asynchronous(cls.post)
            if hasattr(cls, 'put'):
                cls.put = asynchronous(cls.put)
            cls.__init__(self, *args, **kwargs)
    return StreamBody


@stream_body
class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, path):
        address = os.environ['OPENSHIFT_INTERNAL_IP']
        string = urllib2.urlopen("http://%s:15001/" % address).read()
        self.write(string)
        self.finish()

    def put(self, path):
        self.read_bytes = 0
        self.chunk_num = 0
        self.path = path
        self.request.request_continue()
        self.read_chunks()

    def read_chunks(self, chunk=''):
        self.read_bytes += len(chunk)
        chunk_length = min(128 * 1024,  # 128kB in chunk
            self.request.content_length - self.read_bytes)

        if chunk:  # md5.md5(chunk).hexdigest()
            TMP = os.path.join(os.environ['OPENSHIFT_TMP_DIR'], "%s.%d.chunk" % (self.path, self.chunk_num))
            sh.mkdir('-p', sh.dirname(TMP))
            with file(TMP, "wb") as f:
                f.write(chunk)
            self.chunk_num += 1

        if chunk_length > 0:
            self.request.connection.stream.read_bytes(
                chunk_length, self.read_chunks)
        else:
            self.uploaded()

    def uploaded(self):
        self.write('Uploaded %d bytes' % self.read_bytes)
        self.finish()
