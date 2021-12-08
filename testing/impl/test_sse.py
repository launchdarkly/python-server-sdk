from ldclient.impl.sse import _BufferedLineReader, SSEClient

from testing.http_util import ChunkedResponse, start_server

import pytest


class TestBufferedLineReader:    
    @pytest.fixture(params = ["\r", "\n", "\r\n"])
    def terminator(self, request):
        return request.param
    
    @pytest.fixture(params = [
        [
            [ "first line*", "second line*", "3rd line*" ],
            [ "first line", "second line", "3rd line"]
        ],
        [
            [ "*", "second line*", "3rd line*" ],
            [ "", "second line", "3rd line"]
        ],
        [
            [ "first line*", "*", "3rd line*" ],
            [ "first line", "", "3rd line"]
        ],
        [
            [ "first line*", "*", "*", "*", "3rd line*" ],
            [ "first line", "", "", "", "3rd line" ]
        ],
        [
            [ "first line*second line*third", " line*fourth line*"],
            [ "first line", "second line", "third line", "fourth line" ]
        ],        
    ])
    def inputs_outputs(self, terminator, request):
        inputs = list(s.replace("*", terminator).encode() for s in request.param[0])
        return [inputs, request.param[1]]

    def test_parsing(self, inputs_outputs):
        assert list(_BufferedLineReader.lines_from(inputs_outputs[0])) == inputs_outputs[1]

    def test_mixed_terminators(self):
        chunks = [
            b"first line\nsecond line\r\nthird line\r",
            b"\nfourth line\r",
            b"\r\nlast\r\n"
        ]
        expected = [
            "first line",
            "second line",
            "third line",
            "fourth line",
            "",
            "last"
        ]
        assert list(_BufferedLineReader.lines_from(chunks)) == expected


# The tests for SSEClient are fairly basic, just ensuring that it is really making HTTP requests and that the
# API works as expected. The contract test suite is much more thorough - see sse-contract-tests.

class TestSSEClient:
    def test_sends_expected_headers(self):
        with start_server() as server:
            with ChunkedResponse({ 'Content-Type': 'text/event-stream' }) as stream:
                server.for_path('/', stream)
                client = SSEClient(server.uri)

                r = server.await_request()
                assert r.headers['Accept'] == 'text/event-stream'
                assert r.headers['Cache-Control'] == 'no-cache'

    def test_receives_messages(self):
        with start_server() as server:
            with ChunkedResponse({ 'Content-Type': 'text/event-stream' }) as stream:
                server.for_path('/', stream)
                client = SSEClient(server.uri)

                stream.push("event: event1\ndata: data1\n\nevent: event2\ndata: data2\n\n")

                events = client.events

                event1 = next(events)
                assert event1.event == 'event1'
                assert event1.data == 'data1'

                event2 = next(events)
                assert event2.event == 'event2'
                assert event2.data == 'data2'
