from flask import Flask, json, Response, request

import redis

import cPickle as pickle

from ..storages import storages
from .. import settings

import random

app = Flask(__name__)

address = settings.internal_ip
chunks_rs = redis.Redis(host=address, port=15002, db=1)
files_rs = redis.Redis(host=address, port=15002, db=2)


class NSException(Exception):
    pass


@app.route('/')
def index():
    js = json.dumps({"info": "Hello from flask."})
    return Response(js, status=200, mimetype='application/json')


def find_server(hash):
    #TODO:
    old = chunks_rs.lrange(hash, 0, -1)
    if not old:
        return random.choice(storages.keys())
    else:
        return old[0]


@app.route('/chunk/<hash>')
def add_chunk(hash):
    server = find_server(hash)
    chunks_rs.rpush(hash, server)
    js = json.dumps({"result": "OK", 'server': server})

    return Response(js, status=200, mimetype='application/json')


@app.route('/get_chunk/<hash>')
def get_chunk(hash):
    js = json.dumps({"result": "OK", 'chunk': chunks_rs.lrange(hash, 0, -1)})
    return Response(js, status=200, mimetype='application/json')


@app.route('/file/<path:path>', methods=['POST'])
def add_file(path):
    if files_rs.lrange(path, 0, -1):
        js = json.dumps({"result": "ERROR: Entry with this name exists"})
        return Response(js, status=404, mimetype='application/json')
        #files_rs.delete(path)

    splitted = path.split('/')
    for i, folder_name in enumerate(splitted[:-1]):
        folder = '/'.join(splitted[0:i + 1])
        subitem = '/'.join(splitted[0:i + 2])
        if subitem not in files_rs.lrange(folder + "/", 0, -1):
            files_rs.rpush(folder + "/", subitem)

    for item in pickle.loads(request.data):
        files_rs.rpush(path, item)

    js = json.dumps({"result": "OK"})
    return Response(js, status=200, mimetype='application/json')


@app.route('/get_file/<path:path>')
def get_file(path):
    chunks = []
    servers = []

    for hash in files_rs.lrange(path, 0, -1):
        chunks.append(hash)
        try:
            servers.append(random.choice(chunks_rs.lrange(hash, 0, -1)))
        except IndexError:
            raise NSException("Chunk not found")

    js = json.dumps({"result": "OK", "chunks": chunks, "servers": servers})
    return Response(js, status=200, mimetype='application/json')


@app.route('/ls/<path:path>')
def ls(path):
    files = files_rs.lrange(path, 0, -1)
    if files:
        js = json.dumps({"result": "OK", "files": files})
        return Response(js, status=200, mimetype='application/json')
    else:
        js = json.dumps({"result": "ERROR: No such directory"})
        return Response(js, status=404, mimetype='application/json')

if __name__ == '__main__':
    app.run(debug=True, host=address, port=15003)
