from flask import Flask, json, Response, request

import cPickle as pickle
import random

import nslib
from .. import settings
from ..storages import storages

app = Flask(__name__)


@app.route('/')
def index():
    js = json.dumps({"info": "Hello from flask."})
    return Response(js, status=200, mimetype='application/json')


def find_server(hash):
    """
    Returns server to place new chunk
    """
    #TODO:
    old = nslib.chunk_places(hash)
    if not old:
        return random.choice(storages.keys())
    else:
        return old[0]


@app.route('/chunk/<hash>', methods=['POST'])
def add_chunk(hash):
    """
    Registers new chunk in db
    """
    info = pickle.loads(request.data)  # dict

    server = find_server(hash)
    # chunks_rs.rpush(hash, server)
    nslib.set_chunk_size(hash, info['size'])

    js = json.dumps({"result": "OK", 'server': server})

    return Response(js, status=200, mimetype='application/json')


@app.route('/chunk_ready/<hash>', methods=['POST'])
def chunk_ready(hash):
    """
    Marks server as ready with particular chunk
    """
    info = pickle.loads(request.data)  # dict
    nslib.chunk_ready_on_storage(hash, info['storage'])

    js = json.dumps({"result": "OK"})
    return Response(js, status=200, mimetype='application/json')


@app.route('/get_chunk/<hash>')
def get_chunk(hash):
    """
    Returns server where chunk settled
    """
    js = json.dumps({"result": "OK", 'chunk': nslib.chunk_places(hash)})
    return Response(js, status=200, mimetype='application/json')


@app.route('/file/<path:path>', methods=['POST'])
def add_file(path):
    """
    Add file's chunks to db
    """
    path = "/" + path
    chunks = pickle.loads(request.data)

    try:
        nslib.add_file(path, chunks)
    except nslib.FSError, e:
        js = json.dumps({"result": "ERROR %s" % e.message})
        #FIXME: 404?
        return Response(js, status=404, mimetype='application/json')

    js = json.dumps({"result": "OK"})
    return Response(js, status=200, mimetype='application/json')


@app.route('/get_file/<path:path>')
def get_file(path):
    """
    Returns list of chunks and servers to load file
    """
    path = "/" + path

    try:
        chunks, servers = nslib.get_file_chunks(path)
    except nslib.FSError, e:
        js = json.dumps({"result": "ERROR %s" % e.message})
        #FIXME: 404?
        return Response(js, status=404, mimetype='application/json')

    js = json.dumps({"result": "OK", "chunks": chunks, "servers": servers})
    return Response(js, status=200, mimetype='application/json')


@app.route('/ls/', defaults={'path': ''})
@app.route('/ls/<path:path>')
def ls(path):
    """
    Lists directory
    """
    path = "/" + path

    try:
        files = nslib.ls(path)
    except nslib.FSError, e:
        js = json.dumps({"result": "ERROR %s" % e.message})
        #FIXME: 404?
        return Response(js, status=404, mimetype='application/json')

    js = json.dumps({"result": "OK", "files": files})
    return Response(js, status=200, mimetype='application/json')


if __name__ == '__main__':
    app.run(debug=True, host=settings.internal_ip, port=15003)
