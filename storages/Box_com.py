import sh

from . import Storage

import sys
import os
#FIXME
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import settings


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

    def curl_user(self):
        return "%s:%s" % (self.user, self.password)

    @attempts(n=3, catch=sh.ErrorReturnCode)
    def store_chunk(self, chunk_file, hash):
        sh.curl(URL + hash,
            "--user", self.curl_user(),
            "--upload-file", chunk_file)
        sh.rm('-f', chunk_file)

    @attempts(n=3, catch=sh.ErrorReturnCode)
    def get_chunk(self, hash):
        reply = sh.curl(URL + hash,
            "--user", self.curl_user()).stdout
        return reply
