import os
import sys

from ConfigParser import ConfigParser

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import settings

storages = {}
storage_classes = {}


def import_all(file, loc, glob):
    files = os.listdir(os.path.dirname(file))
    for module in list(set([os.path.splitext(f)[0] for f in files if "__init__" not in f])):
        __import__(module, loc, glob)


class Storage(type):
    def __init__(cls, name, bases, dct):
        storage_classes[cls.name()] = cls
        return super(Storage, cls).__init__(name, bases, dct)


def load_storages(file_name):
    conf = ConfigParser()
    if not conf.read(file_name):
        print "No config"
        return
    try:
        storage_sections = conf.get("storages", "list").split(',')
        print storage_sections
        for storage_name in storage_sections:
            stor = storage_classes[conf.get(storage_name.strip(), "type")]()  # Storage class
            stor.identifer = storage_name
            stor.__dict__.update(dict(conf.items(storage_name.strip())))
            storages[storage_name] = stor
    except ConfigParser.Error, e:
        print "ERROR: Broken config, %s" % e.message


import_all(__file__, locals(), globals())
load_storages(os.path.join(settings.datadir, 'secrets'))
