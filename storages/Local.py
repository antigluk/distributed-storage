import os
import sh

from . import Storage

import sys
#FIXME
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import settings


class Local(object):
    __metaclass__ = Storage

    @classmethod
    def name(cls):
        return "local"

    def store_chunk(cls, chunk_file, hash):
        sh.mkdir('-p', os.path.join(settings.datadir, 'local_storage'))
        sh.mv(chunk_file, os.path.join(settings.datadir, 'local_storage', hash))
        sh.rm('-f', chunk_file)

    def get_chunk(cls, hash):
        file_name = os.path.join(settings.datadir, 'local_storage', hash)
        if os.path.exists(file_name):
            return open(file_name).read()
