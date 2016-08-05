import mock
import sys

mock_newrelic = mock.MagicMock()
sys.modules['newrelic'] = mock_newrelic
import newrelic
from ldclient import newrelicwrapper


def setup_function(function):
    try:  # Python 2
        reload(newrelicwrapper)
    except NameError:  # Python 3
        try:
            import imp
            imp.reload(newrelicwrapper)
        except ImportError:  # Python >= 3.4
            import importlib
            importlib.reload(newrelicwrapper)


def teardown_function(function):
    newrelicwrapper.NEWRELIC_ENABLED = True
    newrelic.agent.add_custom_parameter = mock.MagicMock()
    sys.modules['newrelic'] = mock_newrelic


def test_setup():
    assert newrelic == mock_newrelic
    assert newrelicwrapper.NEWRELIC_ENABLED


def test_check_newrelic_enabled_false():
    del sys.modules['newrelic']
    enabled = newrelicwrapper.check_newrelic_enabled()
    assert not enabled


def test_check_newrelic_enabled_true():
    enabled = newrelicwrapper.check_newrelic_enabled()
    assert enabled


def test_check_newrelic_validated():
    del newrelic.agent.add_custom_parameter
    enabled = newrelicwrapper.check_newrelic_enabled()
    assert not enabled


def test_annotate_transaction_noop():
    newrelicwrapper.NEWRELIC_ENABLED = False
    newrelicwrapper.annotate_transaction('key', 'value')
    newrelic.agent.add_custom_parameter.assert_not_called()


def test_annotate_transaction():
    newrelicwrapper.annotate_transaction('key', 'value')
    newrelic.agent.add_custom_parameter.assert_called_with('key', 'value')
