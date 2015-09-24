from __future__ import absolute_import

from copy import deepcopy
from ldclient.util import log, Event
from twisted.internet.defer import Deferred
from twisted.internet.ssl import ClientContextFactory
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.protocols.basic import LineReceiver


class NoValidationContextFactory(ClientContextFactory):
    def getContext(self, *_):
        return ClientContextFactory.getContext(self)


class TwistedSSEClient(object):
    def __init__(self, url, headers, verify, on_event):
        self.url = url + "/features"
        self.verify = verify
        self.headers = headers
        self.on_event = on_event
        self.on_error_retry = 30
        self.running = False
        self.current_request = None

    def reconnect(self, old_protocol):
        """
        :type old_protocol: EventSourceProtocol
        """
        if not self.running:
            return

        retry = old_protocol.retry
        if not retry:
            retry = 5
        from twisted.internet import reactor
        reactor.callLater(retry, self.connect, old_protocol.last_id)

    def start(self):
        self.running = True
        self.connect()

    def connect(self, last_id=None):
        """
        Connect to the event source URL
        """
        headers = deepcopy(self.headers)
        if last_id:
            headers['Last-Event-ID'] = last_id
        headers = dict([(x, [y.encode('utf-8')]) for x, y in headers.items()])
        url = self.url.encode('utf-8')
        from twisted.internet import reactor
        if self.verify:
            agent = Agent(reactor)
        else:
            agent = Agent(reactor, NoValidationContextFactory())

        d = agent.request(
            'GET',
            url,
            Headers(headers),
            None)
        self.current_request = d
        d.addErrback(self.on_connect_error)
        d.addCallback(self.on_response)

    def stop(self):
        if self.running and self.current_request:
            self.current_request.cancel()

    def on_response(self, response):
        from twisted.internet import reactor
        if response.code != 200:
            log.error("non 200 response received: %d" % response.code)
            reactor.callLater(self.on_error_retry, self.connect)
        else:
            finished = Deferred()
            protocol = EventSourceProtocol(self.on_event, finished)
            finished.addBoth(self.reconnect)
            response.deliverBody(protocol)
            return finished

    def on_connect_error(self, ignored):
        """
        :type ignored: twisted.python.Failure
        """
        from twisted.internet import reactor
        ignored.printTraceback()
        log.error("error connecting to endpoint {}: {}".format(self.url, ignored.getTraceback()))
        reactor.callLater(self.on_error_retry, self.connect)


class EventSourceProtocol(LineReceiver):
    def __init__(self, on_event, finished_deferred):
        self.finished = finished_deferred
        self.on_event = on_event
        # Initialize the event and data buffers
        self.event = ''
        self.data = ''
        self.id = None
        self.last_id = None
        self.retry = 5  # 5 second retry default
        self.reset()
        self.delimiter = b'\n'

    def reset(self):
        self.event = 'message'
        self.data = ''
        self.id = None
        self.retry = None

    def lineReceived(self, line):
        if line == '':
            # Dispatch event
            self.dispatch_event()
        else:
            try:
                field, value = line.split(':', 1)
                # If value starts with a space, strip it.
                value = lstrip(value)
            except ValueError:
                # We got a line with no colon, treat it as a field(ignore)
                return

            if field == '':
                # This is a comment; ignore
                pass
            elif field == 'data':
                self.data += value + '\n'
            elif field == 'event':
                self.event = value
            elif field == 'id':
                self.id = value
                pass
            elif field == 'retry':
                self.retry = value
                pass

    def connectionLost(self, *_):
        self.finished.callback(self)

    def dispatch_event(self):
        """
        Dispatch the event
        """
        # If last character is LF, strip it.
        if self.data.endswith('\n'):
            self.data = self.data[:-1]
        log.debug("Dispatching event %s[%s]: %s", self.event, self.id, self.data)
        event = Event(self.data, self.event, self.id, self.retry)
        self.on_event(event)
        if self.id:
            self.last_id = self.id
        self.reset()


def lstrip(value):
    return value[1:] if value.startswith(' ') else value