import pytest

from ldclient.impl.model.attribute_ref import *


class TestAttributeRef:
    @pytest.mark.parametrize("input", ["", "/"])
    def test_invalid_attr_ref_from_path(self, input: str):
        a = AttributeRef.from_path(input)
        assert a.valid is False
        assert a.error is not None
        assert a.depth == 0

    @pytest.mark.parametrize("input", [""])
    def test_invalid_attr_ref_from_literal(self, input: str):
        a = AttributeRef.from_literal(input)
        assert a.valid is False
        assert a.error is not None
        assert a.depth == 0

    @pytest.mark.parametrize("input", ["name", "name/with/slashes", "name~0~1with-what-looks-like-escape-sequences"])
    def test_ref_with_no_leading_slash(self, input: str):
        a = AttributeRef.from_path(input)
        assert a.valid is True
        assert a.error is None
        assert a.depth == 1
        assert a[0] == input

    @pytest.mark.parametrize("input,unescaped", [("/name", "name"), ("/0", "0"), ("/name~1with~1slashes~0and~0tildes", "name/with/slashes~and~tildes")])
    def test_ref_simple_with_leading_slash(self, input: str, unescaped: str):
        a = AttributeRef.from_path(input)
        assert a.valid is True
        assert a.error is None
        assert a.depth == 1
        assert a[0] == unescaped

    @pytest.mark.parametrize("input", ["name", "name/with/slashes", "name~0~1with-what-looks-like-escape-sequences"])
    def test_literal(self, input: str):
        a = AttributeRef.from_literal(input)
        assert a.valid is True
        assert a.error is None
        assert a.depth == 1
        assert a[0] == input

    @pytest.mark.parametrize(
        "input,expected_path",
        [
            ("name", "name"),
            ("name/with/slashes", "name/with/slashes"),
            ("a/b~c", "a/b~c"),
            ("/ssn", "/~1ssn"),
            ("/a~b", "/~1a~0b"),
        ],
    )
    def test_literal_path_escapes_a_leading_slash(self, input: str, expected_path: str):
        # A name that starts with a slash must be escaped, so that a consumer
        # does not read it as a path to a nested property. Any other name is
        # already a valid reference and stays unchanged.
        a = AttributeRef.from_literal(input)
        assert a.path == expected_path
        assert AttributeRef.from_path(a.path).path == expected_path
        assert AttributeRef.from_path(a.path)[0] == input

    def test_get_component(self):
        a = AttributeRef.from_path("/first/sec~1ond/third")
        assert a.depth == 3
        assert a[0] == "first"
        assert a[1] == "sec/ond"
        assert a[2] == "third"
