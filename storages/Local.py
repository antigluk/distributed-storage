import os
import sh

from . import Storage

datadir = os.environ['OPENSHIFT_DATA_DIR']


class Local(object):
    __metaclass__ = Storage

    @classmethod
    def name(cls):
        return "local"

    @classmethod
    def store_chunk(chunk_file, hash):
        sh.mkdir('-p', os.path.join(datadir, 'local_storage'))
        sh.mv(chunk_file, os.path.join(datadir, 'local_storage', hash))
        sh.rm('-f', chunk_file)

    @classmethod
    def get_chunk(hash):
        file_name = os.path.join(datadir, 'local_storage', hash)
        if os.path.exists(file_name):
            return open(file_name).read()
