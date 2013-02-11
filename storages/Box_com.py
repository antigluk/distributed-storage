import os
import sh

from . import Storage

datadir = os.environ['OPENSHIFT_DATA_DIR']

URL = "https://www.box.com/dav/storage/"


class Box_com(object):
    __metaclass__ = Storage

    @classmethod
    def name(cls):
        return "box.com"

    @classmethod
    def store_chunk(cls, chunk_file, hash):
        sh.curl(URL + hash,
            "--user", file(datadir + 'box.net.secrets').read().strip(),
            "--upload-file", chunk_file)
        sh.rm('-f', chunk_file)

    @classmethod
    def get_chunk(cls, hash):
        return str(sh.curl(URL + hash,
            "--user", file(datadir + 'box.net.secrets').read().strip()))
