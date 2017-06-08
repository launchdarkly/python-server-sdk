from ldclient import operators


def test_date_operator():
    assert operators.ops.get("before")(0, 1)
    assert operators.ops.get("before")(-100, 0)
    assert operators.ops.get("before")("1970-01-01T00:00:00Z", 1000)
    assert operators.ops.get("before")("1970-01-01T00:00:00.500Z", 1000)
    # wrong type:
    assert not operators.ops.get("before")(True, 1000)
    assert operators.ops.get("after")("1970-01-01T00:00:02.500Z", 1000)
    # malformed timestamp:
    assert not operators.ops.get("after")("1970-01-01 00:00:02.500Z", 1000)

    assert operators.ops.get("before")("1970-01-01T00:00:02+01:00", 1000)
    assert operators.ops.get("before")(-1000, 1000)

    assert operators.ops.get("after")("1970-01-01T00:00:01.001Z", 1000)
    assert operators.ops.get("after")("1970-01-01T00:00:00-01:00", 1000)

def test_regex_operator():
    assert operators.ops.get("matches")("hello world", "hello.*rld")
    assert operators.ops.get("matches")("hello world", "hello.*rl")
    assert operators.ops.get("matches")("hello world", "l+")
    assert operators.ops.get("matches")("hello world", "(world|planet)")
