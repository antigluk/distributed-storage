import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.httputil
import tornado.httpclient

import os
import sha
import time
from math import ceil
from datetime import datetime
import urllib2
import sys

import sh

from celery import Celery

from storages import storages
import settings
from ns import nslib

address = settings.internal_ip
celery = Celery('tasks', broker='redis://%s:15002/0' % address)


def log(s):
    with file(os.path.join(settings.datadir, 'process_chunk.log'), 'a+') as f:
        f.write("[%s] %s\n" % (datetime.now().strftime("%d_%m_%Y_%H-%M-%S"), s))


@celery.task
def process_chunk(num, chunk_file, hash):
    chunk_size = os.stat(chunk_file).st_size
    log("Storing chunk %d (%s)" % (num, hash))

    try:
        storage_name = nslib.find_server(hash)
        nslib.set_chunk_size(hash, chunk_size)
    except nslib.NSLibException, e:
        log("Failed store chunk %s (%s) for file (%s). Message: %s" %
                (hash, num, chunk_file, e.message))
        return

    storage = storages[storage_name]
    if not nslib.is_chunk_on_storage(hash, storage_name):
        storage.store_chunk(chunk_file, hash)
    else:
        log("Chunk %s (%d) already on storage %s" %
                (hash, num, storage_name))
        sh.rm("-f", chunk_file)

    nslib.chunk_ready_on_storage(hash, storage_name)

    log("Chunk saved %s in %s (%d)" %
            (chunk_file, storage_name, num))


@celery.task
def register_file(path, hashes):
    try:
        nslib.add_file(path, hashes)
    except nslib.FSError, e:
        log("Failed file save %s. Exception: %s" % (path, e.message))
        return

    log("File saved %s" % path)


@celery.task
def remote_upload_file(url, name):
    #http://stackoverflow.com/questions/2028517/python-urllib2-progress-hook

    def upload_handler(response):
        if response.error:
            log("Remote download error [2]: %s" % response.error)
        else:
            log("Remote download %s OK: %s" % (name, response.body))
        tornado.ioloop.IOLoop.instance().stop()
        sh.rm("-f", file_name)

    def chunk_report(bytes_so_far, chunk_size, total_size, pp=[0]):
        percent = float(bytes_so_far) / total_size
        percent = round(percent * 100, 2)
        if ((percent - pp[0]) >= 2):
            pp[0] = percent
            log("Downloaded %d of %d bytes (%0.2f%%)\r" %
                (bytes_so_far, total_size, percent))

        if bytes_so_far >= total_size:
            sys.stdout.write('\n')

    def chunk_read(response, chunk_size=8192, report_hook=None, file_name=None):
        total_size = response.info().getheader('Content-Length').strip()
        total_size = int(total_size)
        bytes_so_far = 0
        log("downloading %d b" % total_size)
        with file(file_name, "w") as f:
            while 1:
                chunk = response.read(chunk_size)
                bytes_so_far += len(chunk)

                if not chunk:
                    break

                f.write(chunk)

                if report_hook:
                    report_hook(bytes_so_far, chunk_size, total_size)

        return bytes_so_far

    # from cStringIO import StringIO

    class Progress(object):
        def __init__(self):
            self._seen = 0.0

        def update(self, total, size, name):
            self._seen += size
            pct = (self._seen / total) * 100.0
            log('%s progress: %.2f' % (name, pct))

    class file_with_callback(file):
        def __init__(self, path, mode, callback, *args):
            file.__init__(self, path, mode)
            self.seek(0, os.SEEK_END)
            self._total = self.tell()
            self.seek(0)
            self._callback = callback
            self._args = args

        def __len__(self):
            return self._total

        def read(self, size):
            data = file.read(self, size)
            self._callback(self._total, len(data), *self._args)
            return data

    log("Remote download initialized: %s, url %s" %
            (name, url))
    file_name = os.path.join(settings.tmpdir, 'cache', name)

    response = urllib2.urlopen(url)
    chunk_read(response, report_hook=chunk_report, file_name=file_name)
    log("Remote download %s: downloaded.")

    # response = urllib2.Request("https://1-antigluk.rhcloud.com/data/remote/%s" % (name))

    progress = Progress()
    stream = file_with_callback(file_name, 'rb', progress.update, file_name)
    req = urllib2.Request("https://1-antigluk.rhcloud.com/data/remote/%s",
        stream, {"Content-Type": "application/octet-stream"})
    # request.add_header('Content-Type', 'your/contenttype')
    req.get_method = lambda: 'PUT'
    try:
        res = urllib2.urlopen(req).read()
        log("uploaded OK: %s" % res)
    except urllib2.HTTPError, e:
        log("uploaded HTTPERROR: code %d %s" % (e.code, e.read()))


class RemoteUploadHandler(tornado.web.RequestHandler):
    def post(self):
        url = self.get_argument('url', None)
        name = self.get_argument('name', sha.sha(url).hexdigest())
        remote_upload_file.delay(url, name)

        self.redirect("/ui/fs/")


class MainHandler(tornado.web.RequestHandler):
    # FIXME: Need to implement all methods for FUSE filesystem.
    # https://github.com/terencehonles/fusepy/blob/master/examples/sftp.py

    # ======= GET ============

    @tornado.web.asynchronous
    def get(self, path):
        # FIXME: authorization
        path = "/" + path
        self.path = path
        self.is_alive = True
        if path[-1] == '/':
            try:
                files = nslib.ls(path)
            except nslib.FSError:
                raise tornado.web.HTTPError(404, "No such file or directory")

            for fl in files:
                self.write("<a href='/data%s'>%s</a><br />" % (fl, fl))
            self.finish()
            return

        try:
            chunks = zip(*nslib.get_file_chunks(path))
        except nslib.FSError:
            raise tornado.web.HTTPError(404, "No such file or directory")

        filename = os.path.split(path)[1]

        log("Start downloading %s, size %s" %
                (path, nslib.get_file_size(path)))

        self.set_header("Content-Type", "application/octet-stream")
        self.set_header("Content-Length", str(nslib.get_file_size(path)))
        self.set_header("Content-Disposition", 'attachment; filename="' + filename + '"')

        self.generator = self.write_generator(chunks)
        tornado.ioloop.IOLoop.instance().add_callback(self.write_callback)

    @tornado.web.asynchronous
    def write_callback(self):
        try:
            data = self.generator.next()
            self.write(data)
            self.flush()
            if self.is_alive:
                tornado.ioloop.IOLoop.instance().add_callback(self.write_callback)
        except StopIteration:
            self.finish()

    def write_generator(self, chunks):
        for chunk, server in chunks:
            data = storages[server].get_chunk(chunk)
            yield data

            log("Chunk %s received from %s (size %d)" %
                    (chunk, server.strip(), len(data)))

    def on_connection_close(self):
        self.is_alive = False
        log("Connection closed %s" % (self.path))

    # ======= PUT ============

    def put(self, path):
        path = "/" + path

        self.path = path
        #FIXME: optimize logging
        log("Uploaded %s size: %d" %
                (path, self.request.content_length))

        self.chunks = self.request.body
        register_file.delay(self.path, self.chunks)
        self.write('Uploaded %d bytes\n' % self.request.content_length)
        self.finish()

#https://gist.github.com/joshmarshall/870216


class BodyStreamHandler(tornado.httpserver.HTTPParseBody):
    content_left = 0

    def __call__(self):
        self.content_left = self.content_length

        self.read_bytes = 0
        self.chunk_num = 0
        # self.chunks = []
        #FIXME: optimize logging

        self.resuming = True
        self.path = self.request.path[len("/data"):]

        self.step = 0
        aligned = 0

        self.prev_available_chunks = settings.available_chunks()

        if not self.request.headers.get("Content-Range"):
            nslib.new_file(self.path)
            self.resuming = False
        else:
            right = int(self.request.headers.get("Content-Range").split("-")[0][len("bytes "):])
            count = int(ceil(right / float(settings.chunk_size)))
            aligned = count * settings.chunk_size
            self.step = aligned - right

            already_uploaded = len(nslib.chunks_for_path(self.path))
            if count > already_uploaded:
                raise Exception("too large offset")
            self.chunk_num = already_uploaded

        log("Start uploading size: %d %s (resume: %s, %d)" %
                (self.content_length, self.path, self.resuming, aligned))

        if self.step:
            self.stream.read_bytes(self.step,  self.read_chunk)
        else:
            self.read_chunk()

    def read_chunk(self, data=None):

        if settings.available_chunks() < settings.chunks_watch_limit:
            #FIXME: advance analysis
            # at least one chunk should be stored while new coming
            if settings.available_chunks() - self.prev_available_chunks >= 0:
                tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 5, self.read_chunk)
                log("Local chunk limit reached. Waiting...")
                return

        self.prev_available_chunks = settings.available_chunks()

        if settings.available_chunks() < settings.chunks_threshold:
            tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 5, self.read_chunk)
            log("Local chunk threshold reached. Waiting...")
            return

        buffer_size = settings.chunk_size
        if self.content_left < buffer_size:
            buffer_size = self.content_left
        self.stream.read_bytes(buffer_size,  self.data_callback)

    def data_callback(self, data=None):
        self.content_left -= len(data)

        hash = sha.sha(data).hexdigest()

        TMP = os.path.join(settings.tmpdir, "cache", "%s.%s.chunk" % (self.chunk_num, hash))
        sh.mkdir('-p', sh.dirname(TMP).strip())
        with file(TMP, "wb") as f:
            f.write(data)
        log("Received %d (%s) %s" %
                (len(data), hash, self.path))

        # self.chunks.append(hash)
        nslib.chunk_received(self.path, hash)
        process_chunk.delay(self.chunk_num, TMP, hash)

        self.chunk_num += 1

        if self.content_left > 0:
            tornado.ioloop.IOLoop.instance().add_callback(self.read_chunk)
        else:
            self.request.body = nslib.chunks_for_path(self.path)  # self.chunks
            self.request.content_length = self.content_length
            nslib.file_done(self.path)
            self.done()
