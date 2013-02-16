import os
import json
import random

import redis

from .. import settings

address = settings.internal_ip
chunks_rs = redis.Redis(host=address, port=15002, db=1)
files_rs = redis.Redis(host=address, port=15002, db=2)
meta_rs = redis.Redis(host=address, port=15002, db=3)


class NSLibException(Exception):
    pass

# ======= Chunks =======


class ChunkError(NSLibException):
    pass


def chunk_places(hash):
    """
    All places of chunk
    """
    return chunks_rs.lrange(hash, 0, -1)


def set_chunk_size(hash, size):
    """
    Updates metadata json "size" entry
    """

    _old = meta_rs.get(hash)
    old = {}
    if _old:
        old = json.loads(_old)

    old.update({"size": size})

    # check for other storages to size collisions
    meta_rs.set(hash, json.dumps(old))


def chunk_ready_on_storage(hash, storage):
    """
    Marks server as ready with particular chunk
    """
    if storage not in chunks_rs.lrange(hash, 0, -1):
        chunks_rs.rpush(hash, storage)


# ======= File system =======


class FSError(NSLibException):
    pass


def exists(path):
    """
    is file exists
    """
    return bool(files_rs.lrange(path, 0, -1))


def mkdirs(path):
    splitted = path.split('/')
    for i, folder_name in enumerate(splitted[:-1]):
        folder = '/'.join(splitted[0:i + 1])
        subitem = '/'.join(splitted[0:i + 2])
        if subitem + "/" not in files_rs.lrange(folder + "/", 0, -1):
            files_rs.rpush(folder + "/", subitem + "/")


def add_file(path, chunks):
    """
    Add file by path with chunks
    """

    path_d, name = os.path.split(path)

    mkdirs(path_d)

    if path not in files_rs.lrange(path_d + "/", 0, -1):
        files_rs.rpush(path_d + "/", path)

    # add chunks
    for item in chunks:
        files_rs.rpush(path, item)


def get_file_chunks(path):
    """
    Returns list of chunks for file
    """
    if exists(path):
        raise FSError("Entry with this name exists")

    chunks = []
    servers = []

    for hash in files_rs.lrange(path, 0, -1):
        chunks.append(hash)
        #FIXME random
        servers.append(random.choice(chunks_rs.lrange(hash, 0, -1)))
    return chunks, servers


def ls(path):
    """
    Lists directory
    """
    if path[-1] != '/':
        raise FSError("Not a directory")

    files = files_rs.lrange(path, 0, -1)
    if not files:
        raise FSError("Not exists or empty directory")

    return files
