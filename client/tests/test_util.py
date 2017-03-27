import pytest

from cook.util import indirect_call

def some_function(a):
    return a

def test_indirect_call():
    assert indirect_call("tests.test_util:some_function", {'a': 1}, {}) is 1
    assert indirect_call("some_function", {'a': 2}, {'some_function': some_function}) is 2
