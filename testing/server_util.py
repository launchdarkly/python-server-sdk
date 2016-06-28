import json
import logging
from queue import Empty
import ssl
import threading

try:
    import queue as queuemod
except:
    import Queue as queuemod

try:
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    # noinspection PyPep8Naming
    import SocketServer as socketserver
    import urlparse
except ImportError:
    # noinspection PyUnresolvedReferences
    from http.server import SimpleHTTPRequestHandler
    # noinspection PyUnresolvedReferences
    import socketserver
    # noinspection PyUnresolvedReferences
    from urllib import parse as urlparse


class TestServer(socketserver.TCPServer):
    allow_reuse_address = True


class GenericServer:

    def __init__(self, host='localhost', use_ssl=False, port=None, cert_file="self_signed.crt",
                 key_file="self_signed.key"):

        self.get_paths = {}
        self.post_paths = {}
        self.raw_paths = {}
        self.stopping = False
        parent = self

        class CustomHandler(SimpleHTTPRequestHandler):

            def handle_request(self, paths):
                # sort so that longest path wins
                for path, handler in sorted(paths.items(), key=lambda item: len(item[0]), reverse=True):
                    if self.path.startswith(path):
                        handler(self)
                        return
                self.send_response(404)
                self.end_headers()

            def do_GET(self):
                self.handle_request(parent.get_paths)

            # noinspection PyPep8Naming
            def do_POST(self):
                self.handle_request(parent.post_paths)

        self.httpd = TestServer(
            ("0.0.0.0", port if port is not None else 0), CustomHandler)
        port = port if port is not None else self.httpd.socket.getsockname()[1]
        self.url = ("https://" if use_ssl else "http://") + host + ":%s" % port
        self.port = port
        logging.info("serving at port %s: %s" % (port, self.url))

        if use_ssl:
            self.httpd.socket = ssl.wrap_socket(self.httpd.socket,
                                                certfile=cert_file,
                                                keyfile=key_file,
                                                server_side=True,
                                                ssl_version=ssl.PROTOCOL_TLSv1)
        self.start()

    def start(self):
        self.stopping = False
        httpd_thread = threading.Thread(target=self.httpd.serve_forever)
        httpd_thread.setDaemon(True)
        httpd_thread.start()

    def stop(self):
        self.shutdown()

    def post_events(self):
        q = queuemod.Queue()

        def do_nothing(handler):
            handler.send_response(200)
            handler.end_headers()

        self.post_paths["/api/events/bulk"] = do_nothing
        self.post_paths["/bulk"] = do_nothing
        return q

    def add_feature(self, data):
        def handle(handler):
            handler.send_response(200)
            handler.send_header('Content-type', 'application/json')
            handler.end_headers()
            handler.wfile.write(json.dumps(data).encode('utf-8'))

        self.get("/api/eval/latest-features", handle)

    def get(self, path, func):
        """
        Registers a handler function to be called when a GET request beginning with 'path' is made.

        :param path: The path prefix to listen on
        :param func: The function to call. Should be a function that takes the querystring as a parameter.
        """
        self.get_paths[path] = func

    def post(self, path, func):
        """
        Registers a handler function to be called when a POST request beginning with 'path' is made.

        :param path: The path prefix to listen on
        :param func: The function to call. Should be a function that takes the post body as a parameter.
        """
        self.post_paths[path] = func

    def shutdown(self):
        self.stopping = True
        self.httpd.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.shutdown()
        finally:
            pass


class SSEServer(GenericServer):

    def __init__(self, host='localhost', use_ssl=False, port=None, cert_file="self_signed.crt",
                 key_file="self_signed.key", queue=queuemod.Queue()):
        GenericServer.__init__(self, host, use_ssl, port, cert_file, key_file)

        def feed_forever(handler):
            handler.send_response(200)
            handler.send_header(
                'Content-type', 'text/event-stream; charset=utf-8')
            handler.end_headers()
            while not self.stopping:
                try:
                    event = queue.get(block=True, timeout=1)
                    """ :type: ldclient.twisted_sse.Event """
                    if event:
                        lines = "event: {event}\ndata: {data}\n\n".format(event=event.event,
                                                                          data=json.dumps(event.data))
                        print("returning {}".format(lines))
                        handler.wfile.write(lines.encode('utf-8'))
                except Empty:
                    pass

        self.get_paths["/"] = feed_forever
        self.queue = queue
