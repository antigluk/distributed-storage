import tornado.web

import os
import urllib2
import sha
import sh
import json
import cPickle as pickle

from celery import Celery

from storages import storages
import settings

address = settings.internal_ip
celery = Celery('tasks', broker='redis://%s:15002/0' % address)


@celery.task
def process_chunk(path, num, chunk_file, hash):
    js = json.loads(urllib2.urlopen("http://%s:15001/chunk/%s" % (address, hash)).read())
    datadir = settings.datadir

    if not js.get('result') == 'OK':
        with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
            f.write("Failed store chunk %s for file %s (%d) with hash %s. Response: %s\n" %
                (chunk_file, path, num, hash, json.dumps(js)))
        return

    #All ok
    storage = storages[js.get('server').strip()]
    storage.store_chunk(chunk_file, hash)

    with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
            f.write("Chunk saved %s in %s for file %s (%d) with hash %s\n" %
                (chunk_file, js.get('server').strip(), path, num, hash))


@celery.task
def register_file(path, hashes):
    url = "http://%s:15001/file/%s" % (address, path)
    # js = json.loads(urllib2.urlopen().read())

    data = pickle.dumps(hashes)
    request = urllib2.Request(url, data, headers={'Content-type': 'application/octet-stream'})
    js = json.loads(urllib2.urlopen(request).read())
    datadir = settings.datadir
    if not js.get('result') == 'OK':
        with file(os.path.join(datadir, 'register_file.log'), 'a+') as f:
            f.write("Failed file save %s. Response: %s\n" % (path, json.dumps(js)))
        return

    with file(os.path.join(datadir, 'register_file.log'), 'a+') as f:
        f.write("File saved %s\n" % path)


def get_chunks_for_file(path):
    url = "http://%s:15001/get_file/%s" % (address, path)
    js = json.loads(urllib2.urlopen(url).read())
    if js['result'] == 'OK':
        return zip(js['chunks'], js['servers'])


def get_files_in_dir(path):
    url = "http://%s:15001/ls/%s" % (address, path)
    js = json.loads(urllib2.urlopen(url).read())
    with file(os.path.join(settings.datadir, 'app.log'), 'a+') as f:
        f.write("ls %s. :%s" % (path, json.dumps(js)))
    if js['result'] == 'OK':
        return js['files']


@tornado.web.stream_body
class MainHandler(tornado.web.RequestHandler):
    # Need to implement all methods for FUSE filesystem.
    # https://github.com/terencehonles/fusepy/blob/master/examples/sftp.py

    def get(self, path):
        datadir = settings.datadir
        if path[-1] == '/':
            for fl in get_files_in_dir(path):
                self.write("<a href='/%s'>%s</a><br />" % (fl, fl))
            self.finish()
            return

        for chunk, server in get_chunks_for_file(path):
            data = storages[server].get_chunk(chunk)
            self.write(data)
            self.flush()

            with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
                f.write("Chunk %s received from %s (size %d)\n" %
                    (chunk, server.strip(), len(data)))

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
        chunk_length = min(settings.chunk_size,
            self.request.content_length - self.read_bytes)

        if chunk:
            TMP = os.path.join(settings.tmpdir, "cache", "%s.%d.chunk" % (self.path, self.chunk_num))
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
