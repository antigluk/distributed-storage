import tornado.web

import os
import sha
import sh

from celery import Celery

from storages import storages
import settings
from ns import nslib

address = settings.internal_ip
celery = Celery('tasks', broker='redis://%s:15002/0' % address)


@celery.task
def process_chunk(path, num, chunk_file, hash):
    datadir = settings.datadir

    chunk_size = os.stat(chunk_file).st_size

    try:
        storage_name = nslib.find_server(hash)
        nslib.set_chunk_size(hash, chunk_size)
    except nslib.NSLibException, e:
        with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
            f.write("Failed store chunk %s for file %s (%d) with hash %s. Message: %s\n" %
                (chunk_file, path, num, hash, e.message))
        return

    storage = storages[storage_name]
    storage.store_chunk(chunk_file, hash)

    nslib.chunk_ready_on_storage(hash, storage_name)

    with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
        f.write("Chunk saved %s in %s for file %s (%d) with hash %s\n" %
            (chunk_file, storage_name, path, num, hash))


@celery.task
def register_file(path, hashes):
    datadir = settings.datadir

    try:
        nslib.add_file(path, hashes)
    except nslib.FSError, e:
        with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
            f.write("Failed file save %s. Exception: %s\n" % (path, e.message))
        return

    with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
        f.write("File saved %s\n" % path)


@tornado.web.stream_body
class MainHandler(tornado.web.RequestHandler):
    # Need to implement all methods for FUSE filesystem.
    # https://github.com/terencehonles/fusepy/blob/master/examples/sftp.py

    def get(self, path):
        path = "/" + path
        datadir = settings.datadir
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
            chunks = nslib.get_file_chunks(path)
        except nslib.FSError:
            raise tornado.web.HTTPError(404, "No such file or directory")
        for chunk, server in chunks:
            data = storages[server].get_chunk(chunk)
            self.write(data)
            self.flush()

            with file(os.path.join(datadir, 'process_chunk.log'), 'a+') as f:
                f.write("Chunk %s received from %s (size %d)\n" %
                    (chunk, server.strip(), len(data)))

        self.finish()

    def put(self, path):
        path = "/" + path
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
            TMP = os.path.join(settings.tmpdir, "cache", "%s.%d.chunk" % (self.path[1:], self.chunk_num))
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
