from flask import Flask, json, Response, request

import redis
import os

import cPickle as pickle

from ..storages import storages

app = Flask(__name__)

address = os.environ['OPENSHIFT_INTERNAL_IP']
chunks_rs = redis.Redis(host=address, port=15002, db=1)
files_rs = redis.Redis(host=address, port=15002, db=2)


@app.route('/')
def index():
    js = json.dumps({"info": "Hello from flask."})
    return Response(js, status=200, mimetype='application/json')


def find_server():
    #TODO:
    return storages.keys()[0]


@app.route('/chunk/<hash>')
def add_chunk(hash):
    server = find_server()
    chunks_rs.set(hash, server)
    js = json.dumps({"result": "OK", 'server': server})

    return Response(js, status=200, mimetype='application/json')


@app.route('/get_chunk/<hash>')
def get_chunk(hash):
    chunks_rs.get(hash)
    js = json.dumps({"result": "OK"})
    return Response(js, status=200, mimetype='application/json')


@app.route('/file/<path:path>', methods=['POST'])
def add_file(path):
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
        servers.append(chunks_rs.get(hash))

    js = json.dumps({"result": "OK", "chunks": chunks, "servers": servers})
    return Response(js, status=200, mimetype='application/json')


if __name__ == '__main__':
    app.run(debug=True, host=address, port=15003)
