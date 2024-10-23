import json
import queue
import socket
import ssl
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from ssl import PROTOCOL_TLSv1_2, SSLContext
from threading import Thread


def get_available_port():
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    _, port = s.getsockname()
    s.close()
    return port


def poll_until_started(port):
    deadline = time.time() + 1
    while time.time() < deadline:
        s = socket.socket()
        try:
            s.connect(('localhost', port))
            return
        except socket.error:
            pass
        finally:
            s.close()
        time.sleep(0.05)
    raise Exception("test server on port %d was not reachable" % port)


def start_server():
    sw = MockServerWrapper(get_available_port(), False)
    sw.start()
    poll_until_started(sw.port)
    return sw


def start_secure_server():
    sw = MockServerWrapper(get_available_port(), True)
    sw.start()
    poll_until_started(sw.port)
    return sw


class MockServerWrapper(Thread):
    def __init__(self, port, secure):
        Thread.__init__(self, name="ldclient.testing.mock-server-wrapper")
        self.port = port
        self.uri = '%s://localhost:%d' % ('https' if secure else 'http', port)
        self.server = HTTPServer(('localhost', port), MockServerRequestHandler)
        if secure:
            context = SSLContext(PROTOCOL_TLSv1_2)
            context.load_cert_chain('./ldclient/testing/selfsigned.pem', './ldclient/testing/selfsigned.key')
            self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.server.server_wrapper = self
        self.matchers = {}
        self.requests = queue.Queue()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def run(self):
        self.server.serve_forever(0.1)  # 0.1 seconds is how often it'll check to see if it is shutting down

    def for_path(self, uri_path, content):
        self.matchers[uri_path] = content
        return self

    def await_request(self):
        return self.requests.get()

    def require_request(self):
        return self.requests.get(block=False)

    def wait_until_request_received(self):
        req = self.requests.get()
        self.requests.put(req)

    def should_have_requests(self, count):
        if self.requests.qsize() != count:
            rs = []
            while not self.requests.empty():
                rs.append(str(self.requests.get(False)))
            assert False, "expected %d more requests but had %s" % (count, rs)

    # enter/exit magic methods allow server to be auto-closed by "with" statement
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class MockServerRequestHandler(BaseHTTPRequestHandler):
    def do_CONNECT(self):
        self._do_request()

    def do_GET(self):
        self._do_request()

    def do_POST(self):
        self._do_request()

    def _do_request(self):
        server_wrapper = self.server.server_wrapper
        server_wrapper.requests.put(MockServerRequest(self))
        handler = server_wrapper.matchers.get(self.path)
        if handler:
            handler.write(self)
        else:
            self.send_error(404)


class MockServerRequest:
    def __init__(self, request):
        self.method = request.command
        self.path = request.path
        self.headers = request.headers
        content_length = int(request.headers.get('content-length', 0))
        if content_length:
            self.body = request.rfile.read(content_length).decode('UTF-8')
        else:
            self.body = None

    def __str__(self):
        return "%s %s" % (self.method, self.path)


class BasicResponse:
    def __init__(self, status, body=None, headers=None):
        self.status = status
        self.body = body
        self.headers = headers or {}

    def add_headers(self, headers):
        for key, value in (headers or {}).items():
            self.headers[key] = value

    def write(self, request):
        request.send_response(self.status)
        for key, value in self.headers.items():
            request.send_header(key, value)
        request.end_headers()
        if self.body:
            request.wfile.write(self.body.encode('UTF-8'))


class JsonResponse(BasicResponse):
    def __init__(self, data, headers=None):
        h = headers or {}
        h.update({'Content-Type': 'application/json'})
        BasicResponse.__init__(self, 200, json.dumps(data or {}), h)


class ChunkedResponse:
    def __init__(self, headers=None):
        self.queue = queue.Queue()
        self.headers = headers or {}

    def push(self, chunk):
        if chunk is not None:
            self.queue.put(chunk)

    def close(self):
        self.queue.put(None)

    def write(self, request):
        request.send_response(200)
        request.send_header('Transfer-Encoding', 'chunked')
        for key, value in self.headers.items():
            request.send_header(key, value)
        request.end_headers()
        request.wfile.flush()
        while True:
            chunk = self.queue.get()
            if chunk is None:
                request.wfile.write('0\r\n\r\n'.encode('UTF-8'))
                request.wfile.flush()
                break
            else:
                request.wfile.write(('%x\r\n%s\r\n' % (len(chunk), chunk)).encode('UTF-8'))
                request.wfile.flush()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class CauseNetworkError:
    def write(self, request):
        raise Exception('intentional error')


class SequentialHandler:
    def __init__(self, *argv):
        self.handlers = argv
        self.counter = 0

    def write(self, request):
        handler = self.handlers[self.counter]
        if self.counter < len(self.handlers) - 1:
            self.counter += 1
        handler.write(request)
