from client_entity import ClientEntity

import json
import logging
import os
import sys
from flask import Flask, request
from flask.logging import default_handler
from logging.config import dictConfig
from werkzeug.exceptions import HTTPException


default_port = 8000


# logging configuration
dictConfig({
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] [%(name)s] %(levelname)s: %(message)s',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console']
    },
    'loggers': {
        'ldclient': {
            'level': 'INFO', # change to 'DEBUG' to enable SDK debug logging
        },
        'werkzeug': { 'level': 'ERROR' } # disable irrelevant Flask app logging
    }
})

app = Flask(__name__)
app.logger.removeHandler(default_handler)

client_counter = 0
clients = {}
global_log = logging.getLogger('testservice')


@app.errorhandler(Exception)
def handle_exception(e):
    # pass through HTTP errors
    if isinstance(e, HTTPException):
        return e

    app.logger.exception(e)
    return str(e), 500

@app.route('/', methods=['GET'])
def status():
    body = {
        'capabilities': [
            'server-side',
            'server-side-polling',
            'all-flags-with-reasons',
            'all-flags-client-side-only',
            'all-flags-details-only-for-tracked-flags',
            'big-segments',
            'context-type',
            'secure-mode-hash',
            'tags',
            'migrations',
            'event-sampling'
        ]
    }
    return (json.dumps(body), 200, {'Content-type': 'application/json'})

@app.route('/', methods=['DELETE'])
def delete_stop_service():
    global_log.info("Test service has told us to exit")
    os._exit(0)

@app.route('/', methods=['POST'])
def post_create_client():
    global client_counter, clients

    options = request.get_json()

    client_counter += 1
    client_id = str(client_counter)
    resource_url = '/clients/%s' % client_id

    client = ClientEntity(options['tag'], options['configuration'])

    if client.is_initializing() is False and options['configuration'].get('initCanFail', False) is False:
        client.close()
        return ("Failed to initialize", 500)

    clients[client_id] = client
    return ('', 201, {'Location': resource_url})


@app.route('/clients/<id>', methods=['POST'])
def post_client_command(id):
    global clients

    params = request.get_json()

    client = clients[id]
    if client is None:
        return ('', 404)

    command = params.get('command')
    sub_params = params.get(command)

    response = None

    if command == "evaluate":
        response = client.evaluate(sub_params)
    elif command == "evaluateAll":
        response = client.evaluate_all(sub_params)
    elif command == "customEvent":
        client.track(sub_params)
    elif command == "identifyEvent":
        client.identify(sub_params)
    elif command == "flushEvents":
        client.flush()
    elif command == "secureModeHash":
        response = client.secure_mode_hash(sub_params)
    elif command == "contextBuild":
        response = client.context_build(sub_params)
    elif command == "contextConvert":
        response = client.context_convert(sub_params)
    elif command == "getBigSegmentStoreStatus":
        response = client.get_big_segment_store_status()
    elif command == "migrationVariation":
        response = client.migration_variation(sub_params)
    elif command == "migrationOperation":
        response = client.migration_operation(sub_params)
    else:
        return ('', 400)

    if response is None:
        return ('', 201)
    return (json.dumps(response), 200)

@app.route('/clients/<id>', methods=['DELETE'])
def delete_client(id):
    global clients

    client = clients[id]
    if client is None:
        return ('', 404)

    client.close()
    return ('', 202)

if __name__ == "__main__":
    port = default_port
    if sys.argv[len(sys.argv) - 1] != 'service.py':
        port = int(sys.argv[len(sys.argv) - 1])
    global_log.info('Listening on port %d', port)
    app.run(host='0.0.0.0', port=port)
