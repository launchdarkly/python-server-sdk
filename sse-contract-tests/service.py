from stream_entity import StreamEntity

import json
import logging
import os
import sys
import urllib3
from flask import Flask, request
from flask.logging import default_handler
from logging.config import dictConfig

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
        'werkzeug': { 'level': 'ERROR' } # disable irrelevant Flask app logging
    }
})

app = Flask(__name__)
app.logger.removeHandler(default_handler)

stream_counter = 0
streams = {}
global_log = logging.getLogger('testservice')

http_client = urllib3.PoolManager()

@app.route('/', methods=['GET'])
def status():
    body = {
        'capabilities': [
            'headers',
            'last-event-id'
        ]
    }
    return (json.dumps(body), 200, {'Content-type': 'application/json'})

@app.route('/', methods=['DELETE'])
def delete_stop_service():
    print("Test service has told us to exit")
    quit()

@app.route('/', methods=['POST'])
def post_create_stream():
    global stream_counter, streams

    options = json.loads(request.data)

    stream_counter += 1
    stream_id = str(stream_counter)
    resource_url = '/streams/%s' % stream_id

    stream = StreamEntity(options)
    streams[stream_id] = stream

    return ('', 201, {'Location': resource_url})

@app.route('/streams/<id>', methods=['DELETE'])
def delete_stream(id):
    global streams

    stream = streams[id]
    if stream is None:
        return ('', 404)
    stream.close()
    return ('', 202)

if __name__ == "__main__":
    port = default_port
    if sys.argv[len(sys.argv) - 1] != 'service.py':
        port = int(sys.argv[len(sys.argv) - 1])
    global_log.info('Listening on port %d', port)
    app.run(host='0.0.0.0', port=port)
