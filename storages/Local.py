from . import Storage


class Local(object):
    __metaclass__ = Storage

    @classmethod
    def name(cls):
        return "local"
