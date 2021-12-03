import json
import logging
import os
import sys
import threading
import traceback
import urllib3

# Import ldclient from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from ldclient.config import HTTPConfig
from ldclient.impl.http import HTTPFactory
from ldclient.impl.sse import SSEClient

port = 8000

stream_counter = 0
streams = {}

http_client = urllib3.PoolManager()

class StreamEntity:
    def __init__(self, options):
        self.options = options
        self.callback_url = options["callbackUrl"]
        self.log = logging.getLogger(options["tag"])
        self.closed = False
        self.callback_counter = 0
        
        thread = threading.Thread(target=self.run)
        thread.start()

    def run(self):
        stream_url = self.options["streamUrl"]
        http_factory = HTTPFactory(
            self.options.get("headers", {}),
            HTTPConfig(read_timeout =
                None if self.options.get("readTimeoutMs") is None else
                    self.options["readTimeoutMs"] / 1000)
        )
        try:
            self.log.info('Opening stream from %s', stream_url)
            sse = SSEClient(
                stream_url,
                retry =
                    None if self.options.get("initialDelayMs") is None else
                        self.options.get("initialDelayMs") / 1000,
                last_id = self.options.get("lastEventId"),
                http_factory = http_factory
                )
            self.sse = sse
            for message in sse.events:
                self.log.info('Received event from stream (%s)', message.event)
                self.send_message({
                    'kind': 'event',
                    'event': {
                        'type': message.event,
                        'data': message.data,
                        'id': message.last_event_id
                    }
                })
            self.send_message({
                'kind': 'error',
                'error': 'Stream closed'
            })
        except Exception as e:
            self.log.info('Received error from stream: %s', e)
            self.log.debug(traceback.format_exc())
            self.send_message({
                'kind': 'error',
                'error': str(e)
            })

    def send_message(self, message):
        global http_client

        if self.closed:
            return
        self.callback_counter += 1
        callback_url = "%s/%d" % (self.options["callbackUrl"], self.callback_counter)

        try:
            resp = http_client.request(
                'POST',
                callback_url,
                headers = {'Content-Type': 'application/json'},
                body = json.dumps(message)
                )
            if resp.status >= 300 and not self.closed:
                self.log.error('Callback request returned HTTP error %d', resp.status)
        except Exception as e:
            if not self.closed:
                self.log.error('Callback request failed: %s', e)

    def close(self):
        # how to close the stream??
        self.closed = True
        self.log.info('Test ended')
