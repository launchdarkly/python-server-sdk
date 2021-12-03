from ldclient.impl.sse import _BufferedLineReader

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


class TestSSEClient:
    pass
