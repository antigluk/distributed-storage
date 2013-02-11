import tornado.web

import os
import urllib2
import sha
import sh
import json
import cPickle as pickle

from celery import Celery

from storages import storages

address = os.environ['OPENSHIFT_INTERNAL_IP']
celery = Celery('tasks', broker='redis://%s:15002/0' % address)


@celery.task
def process_chunk(path, num, chunk_file, hash):
    address = os.environ['OPENSHIFT_INTERNAL_IP']
    js = json.loads(urllib2.urlopen("http://%s:15001/chunk/%s" % (address, hash)).read())
    datadir = os.environ['OPENSHIFT_DATA_DIR']

    if not js.get('result') == 'OK':
        with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
            f.write("Failed store chunk %s for file %s (%d) with hash %s\n" %
                (chunk_file, path, num, hash))
        return

    #All ok
    storage = storages[js.get('server').strip()]
    storage.store_chunk(chunk_file, hash)

    with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
            f.write("Chunk saved %s for file %s (%d) with hash %s\n" %
                (chunk_file, path, num, hash))


@celery.task
def register_file(path, hashes):
    address = os.environ['OPENSHIFT_INTERNAL_IP']
    url = "http://%s:15001/file/%s" % (address, path)
    # js = json.loads(urllib2.urlopen().read())

    data = pickle.dumps(hashes)
    request = urllib2.Request(url, data, headers={'Content-type': 'application/octet-stream'})
    js = json.loads(urllib2.urlopen(request).read())
    if not js.get('result') == 'OK':
        datadir = os.environ['OPENSHIFT_DATA_DIR']
        with file(os.path.join(datadir, 'register_file.log'), 'a+') as f:
            f.write("Failed file save %s" % path)
        return

    with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
        f.write("File saved %s" % path)


def get_chunks_for_file(path):
    address = os.environ['OPENSHIFT_INTERNAL_IP']
    url = "http://%s:15001/file/%s" % (address, path)
    js = json.loads(urllib2.urlopen(url).read())
    if js['result'] == 'OK':
        return js['data']


@tornado.web.stream_body
class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, path):
        for chunk, server in get_chunks_for_file(path).iteritems():
            self.write(storages[server].get_chunk(chunk))

        self.finish()

    def put(self, path):
        self.read_bytes = 0
        self.chunk_num = 0
        self.path = path
        self.request.request_continue()
        self.read_chunks()
        self.chunks = []

    def read_chunks(self, chunk=''):
        self.read_bytes += len(chunk)
        chunk_length = min(128 * 1024,  # 128kB in chunk
            self.request.content_length - self.read_bytes)

        if chunk:
            TMP = os.path.join(os.environ['OPENSHIFT_TMP_DIR'], "cache", "%s.%d.chunk" % (self.path, self.chunk_num))
            sh.mkdir('-p', sh.dirname(TMP).strip())
            with file(TMP, "wb") as f:
                f.write(chunk)

            hash = sha.sha(chunk).hexdigest()
            self.chunks.append(hash)
            process_chunk.delay(self.path, self.chunk_num, TMP, hash)

            self.chunk_num += 1

        if chunk_length > 0:
            self.request.connection.stream.read_bytes(
                chunk_length, self.read_chunks)
        else:
            self.uploaded()

    def uploaded(self):
        self.write('Uploaded %d bytes' % self.read_bytes)
        register_file.delay(self.path, self.chunks)
        self.finish()
