import pytest

from ldclient import operators


@pytest.mark.parametrize("op,value1,value2,expected", [
    # numeric comparisons
    [ "in",                 99,      99,      True ],
    [ "in",                 99.0001, 99.0001, True ],
    [ "in",                 99,      99.0001, False ],
    [ "in",                 99.0001, 99,      False ],
    [ "lessThan",           99,      99.0001, True ],
    [ "lessThan",           99.0001, 99,      False ],
    [ "lessThan",           99,      99,      False ],
    [ "lessThanOrEqual",    99,      99.0001, True ],
    [ "lessThanOrEqual",    99.0001, 99,      False ],
    [ "lessThanOrEqual",    99,      99,      True ],
    [ "greaterThan",        99.0001, 99,      True ],
    [ "greaterThan",        99,      99.0001, False ],
    [ "greaterThan",        99,      99,      False ],
    [ "greaterThanOrEqual", 99.0001, 99,      True ],
    [ "greaterThanOrEqual", 99,      99.0001, False ],
    [ "greaterThanOrEqual", 99,      99,      True ],

    # string comparisons
    [ "in",         "x",   "x",   True ],
    [ "in",         "x",   "xyz", False ],
    [ "startsWith", "xyz", "x",   True ],
    [ "startsWith", "x",   "xyz", False ],
    [ "endsWith",   "xyz", "z",   True ],
    [ "endsWith",   "z",   "xyz", False ],
    [ "contains",   "xyz", "y",   True ],
    [ "contains",   "y",   "xyz", False ],

    # mixed strings and numbers
    [ "in",                 "99", 99,   False ],
    [ "in",                 99,   "99", False ],
    [ "contains",           "99", 99,   False ],
    [ "startsWith",         "99", 99,   False ],
    [ "endsWith",           "99", 99,   False ],
    [ "lessThanOrEqual",    "99", 99,   False ],
    [ "lessThanOrEqual",    99,   "99", False ],
    [ "greaterThanOrEqual", "99", 99,   False ],
    [ "greaterThanOrEqual", 99,   "99", False ],

    # regex
    [ "matches", "hello world", "hello.*rld",     True ],
    [ "matches", "hello world", "hello.*rl",      True ],
    [ "matches", "hello world", "l+",             True ],
    [ "matches", "hello world", "(world|planet)", True ],
    [ "matches", "hello world", "aloha",          False ],
    # [ "matches", "hello world", "***not a regex", False ],   # currently throws an exception

    # dates
    [ "before", 0, 1,                              True ],
    [ "before", -100, 0,                           True ],
    [ "before", "1970-01-01T00:00:00Z", 1000,      True ],
    [ "before", "1970-01-01T00:00:00.500Z", 1000,  True ],
    [ "before", True, 1000,                        False ],  # wrong type
    [ "after",  "1970-01-01T00:00:02.500Z", 1000,  True ],
    [ "after",  "1970-01-01 00:00:02.500Z", 1000,  False ],  # malformed timestamp
    [ "before", "1970-01-01T00:00:02+01:00", 1000, True ],
    [ "before", -1000, 1000,                       True ],
    [ "after",  "1970-01-01T00:00:01.001Z", 1000,  True ],
    [ "after",  "1970-01-01T00:00:00-01:00", 1000, True ],

    # semver
    [ "semVerEqual",       "2.0.1", "2.0.1",    True ],
    [ "semVerEqual",       "2.0",   "2.0.0",    True ],
    [ "semVerEqual",       "2",     "2.0.0",    True ],
    [ "semVerEqual",       "2.0-rc1", "2.0.0-rc1", True ],
    [ "semVerLessThan",    "2.0.0", "2.0.1",    True ],
    [ "semVerLessThan",    "2.0",   "2.0.1",    True ],
    [ "semVerLessThan",    "2.0.1", "2.0.0",    False ],
    [ "semVerLessThan",    "2.0.1", "2.0",      False ],
    [ "semVerGreaterThan", "2.0.1", "2.0.0",    True ],
    [ "semVerGreaterThan", "2.0.1", "2.0",      True ],
    [ "semVerGreaterThan", "2.0.0", "2.0.1",    False ],
    [ "semVerGreaterThan", "2.0",   "2.0.1",    False ],
    [ "semVerLessThan",    "2.0.1", "xbad%ver", False ],
    [ "semVerGreaterThan", "2.0.1", "xbad%ver", False ]
])
def test_operator(op, value1, value2, expected):
    assert operators.ops.get(op)(value1, value2) == expected
