import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.httputil

import os
import sha
import time
from math import ceil
import datetime

import sh

from celery import Celery

from storages import storages
import settings
from ns import nslib

address = settings.internal_ip
celery = Celery('tasks', broker='redis://%s:15002/0' % address)


def log(s):
    with file(os.path.join(settings.datadir, 'process_chunk.log'), 'a+') as f:
        f.write("[%s] %s" % (datetime.now().strftime("%d_%m_%Y_%H-%M-%S"), s))


@celery.task
def process_chunk(num, chunk_file, hash):
    chunk_size = os.stat(chunk_file).st_size

    try:
        storage_name = nslib.find_server(hash)
        nslib.set_chunk_size(hash, chunk_size)
    except nslib.NSLibException, e:
        log("Failed store chunk %s (%s) for file (%s). Message: %s\n" %
                (hash, num, chunk_file, e.message))
        return

    storage = storages[storage_name]
    if not nslib.is_chunk_on_storage(hash, storage_name):
        storage.store_chunk(chunk_file, hash)
    else:
        log("Chunk %s (%d) already on storage %s\n" %
                (hash, num, storage_name))
        sh.rm("-f", chunk_file)

    nslib.chunk_ready_on_storage(hash, storage_name)

    log("Chunk saved %s in %s (%d)\n" %
            (chunk_file, storage_name, num))


@celery.task
def register_file(path, hashes):
    try:
        nslib.add_file(path, hashes)
    except nslib.FSError, e:
        log("Failed file save %s. Exception: %s\n" % (path, e.message))
        return

    log("File saved %s\n" % path)


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

        log("Start downloading %s, size %s\n" %
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

            log("Chunk %s received from %s (size %d)\n" %
                    (chunk, server.strip(), len(data)))

    def on_connection_close(self):
        self.is_alive = False
        log("Connection closed %s\n" % (self.path))

    # ======= PUT ============

    def put(self, path):
        path = "/" + path

        self.path = path
        #FIXME: optimize logging
        log("Uploaded %s size: %d\n" %
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

        log("Start uploading size: %d %s (resume: %s, %d)\n" %
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
                log("Local chunk limit reached. Waiting...\n")
                return

        self.prev_available_chunks = settings.available_chunks()

        if settings.available_chunks() < settings.chunks_threshold:
            tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 5, self.read_chunk)
            log("Local chunk threshold reached. Waiting...\n")
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
        log("Received %d (%s) %s\n" %
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
