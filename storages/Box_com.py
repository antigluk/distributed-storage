import os
import sh

from . import Storage

datadir = os.environ['OPENSHIFT_DATA_DIR']

URL = "https://www.box.com/dav/storage/"


def attempts(n=3, catch=Exception):
    def _attempts(f):
        def __attempts(*args, **kwargs):
            attempts = n
            result = None
            while True:
                try:
                    result = f(*args, **kwargs)
                except catch, e:
                    if attempts < 0:
                        raise e
                else:
                    break
                attempts -= 1
            return result

        return __attempts
    return _attempts


class Box_com(object):
    __metaclass__ = Storage

    @classmethod
    def name(cls):
        return "box.com"

    @classmethod
    @attempts(n=3, catch=sh.ErrorReturnCode)
    def store_chunk(cls, chunk_file, hash):
        sh.curl(URL + hash,
            "--user", file(datadir + 'box.net.secrets').read().strip(),
            "--upload-file", chunk_file)
        sh.rm('-f', chunk_file)

    @classmethod
    @attempts(n=3, catch=sh.ErrorReturnCode)
    def get_chunk(cls, hash):
        reply = sh.curl(URL + hash,
            "--user", file(datadir + 'box.net.secrets').read().strip()).stdout
        return reply
