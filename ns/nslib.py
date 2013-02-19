import os
import sys
import json
import random
import time

import redis

import cPickle as pickle

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import settings
from storages import storages

address = settings.internal_ip
chunks_rs = redis.Redis(host=address, port=15002, db=1)
files_rs = redis.Redis(host=address, port=15002, db=2)
meta_rs = redis.Redis(host=address, port=15002, db=3)
files_temp_rs = redis.Redis(host=address, port=15002, db=4)


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

    # FIXME: check for other storages to hash/size collisions
    meta_rs.set(hash, json.dumps(old))


def get_chunk_size(hash):
    """
    Returns metadata json "size" entry
    """

    data = meta_rs.get(hash)
    if data:
        data = json.loads(data)
        return data['size']
    else:
        raise ChunkError("chunk %s not exists" % hash)


def set_file_size(path, size):
    """
    Updates metadata json "size" entry
    """

    _old = meta_rs.get(path)
    old = {}
    if _old:
        old = json.loads(_old)

    old.update({"size": size})

    # FIXME: check for other storages to hash/size collisions
    meta_rs.set(path, json.dumps(old))


def get_file_size(path):
    """
    Returns metadata json "size" entry
    """

    data = meta_rs.get(path)
    if data:
        data = json.loads(data)
        return data['size']
    else:
        chunks, _ = get_file_chunks(path)
        size = 0
        for chunk in chunks:
            size += get_chunk_size(chunk)
        set_file_size(path, size)
        return size


def chunk_ready_on_storage(hash, storage):
    """
    Marks server as ready with particular chunk
    """
    if storage not in chunks_rs.lrange(hash, 0, -1):
        chunks_rs.rpush(hash, storage)


def is_chunk_on_storage(hash, storage):
    return storage in chunks_rs.lrange(hash, 0, -1)


# ======== Resuming support ==========


def new_file(path):
    files_temp_rs.delete(path)


def chunk_received(path, hash):
    if hash not in chunks_for_path(path):
        files_temp_rs.rpush(path, hash)


def chunks_for_path(path):
    return files_temp_rs.lrange(path, 0, -1)

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
    if path_d == "/":
        path_d = ""

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
    if not exists(path):
        raise FSError("Entry with this name not exists")

    chunks = []
    servers = []

    for hash in files_rs.lrange(path, 0, -1):
        chunks.append(hash)
        #FIXME: random
        servers.append(random.choice(chunks_rs.lrange(hash, 0, -1)))
    return chunks, servers


def ls(path):
    """
    Lists directory
    """
    #FIXME: returns NOT EXISTS on empty directory
    if path[-1] != '/':
        raise FSError("Not a directory")

    files = files_rs.lrange(path, 0, -1)
    if not files:
        raise FSError("Not exists or empty directory")

    return files


def find_server(hash):
    """
    Returns server to place new chunk
    """
    old = chunk_places(hash)
    if not old or (old[0] not in storages.keys()):
        #FIXME: need to check free space on storage
        s_list, full_info = scan_stats()
        #>5*chunk MB free space
        min_size = 5 * settings.chunk_size
        # s_list = filter(lambda serv: (float(serv['size']) - float(serv['used'])) > min_size, s_list)
        if not s_list:
            raise ChunkError("All storages FULL!")
        return sorted(s_list, key=lambda x: x['chunks_count'])[0]['name']
        # return random.choice(storages.keys())
    else:
        return old[0]


def used_size_on_storage(storage):
    used = 0L
    count = 0L
    for chunk in chunks_rs.keys():
        if storage in chunks_rs.lrange(chunk, 0, -1):
            count += 1
            used += json.loads(meta_rs.get(chunk))["size"]
    return used, count


def full_info():
    #FIXME: optimization point.
    #  temporary size storage, recalculate every hour/day etc
    used = 0L
    size = 0L
    count = 0L
    for storage in storages.keys():
        size += int(storages[storage].allow_space) * 1024 * 1024L
        used_p, count_p = used_size_on_storage(storage)
        used += used_p
        count += count_p
    fs_items = len(files_rs.keys())
    return size, used, count, fs_items


def scan_stats(cached=True):
    #FIXME: return result in float, not strings
    #FIXME: optimization point. need to remove cache
    #  temporary stats storage, recalculate every hour/day etc
    #Run this by cron, with cached=False
    TMP_STATS = os.path.join(settings.tmpdir, "fs_stats.dat")
    LOCK_FILE = os.path.join(settings.tmpdir, "fs_stats.lock")

    if (cached and os.path.exists(TMP_STATS) and \
        os.stat(TMP_STATS).st_atime < 60) or os.path.exists(LOCK_FILE):

        stats = open(TMP_STATS).read()
        if stats:
            return pickle.loads(stats)

    with file(LOCK_FILE, "w") as f:
        f.write("")

    s_list = []
    for storage in storages.values():
        used, chunk_num = used_size_on_storage(storage.identifer)
        s_list.append({"name": storage.identifer,
                       "size": "%.2f" % (float(storage.allow_space)),
                       "used": "%.2f" % (float(used) / 1024 / 1024),
                       "free": "%.2f" % ((float(storage.allow_space) - float(used) / 1024 / 1024)),
                       "chunks_count": chunk_num,  # storage.chunks_count(),
                       })

    size, used, count, fs_items = full_info()
    info = {"size": "%.2f" % (float(size) / 1024 / 1024),
            "used": "%.2f" % (float(used) / 1024 / 1024),
            "count": count,
            "free": "%.2f" % (float(size - used) / 1024 / 1024),
            "fs_items": fs_items,
           }
    pickle.dump((s_list, info,), open(TMP_STATS, "w"))

    os.remove(LOCK_FILE)

    return s_list, info


def fsck():
    # 1) check all chunks in files exists
    pass
    # 2) check all chunks owns by some file (danger! could be during file uploading)
    pass
