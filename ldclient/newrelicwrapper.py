from __future__ import absolute_import

from ldclient.util import log

def check_newrelic_enabled():
    try:
        import newrelic
        if hasattr(newrelic, 'agent') and hasattr(newrelic.agent, 'add_custom_parameter'):
            return True
    except ImportError:
        pass
    return False
NEWRELIC_ENABLED = check_newrelic_enabled()


def annotate_transaction(key, value):
    # Make sure "key" is not a New Relic Insights' reserved word
    # https://docs.newrelic.com/docs/insights/new-relic-insights/decorating-events/insights-custom-attributes#keywords
    if NEWRELIC_ENABLED:
        import newrelic
        newrelic.agent.add_custom_parameter(str(key), str(value))

def annotate_newrelic(func):
    def wrapped_func(*args, **kwargs):
        value = func(*args, **kwargs)
        try:
            annotate_transaction(args[1], value)
        except Exception:
            log.exception('Unhandled exception from annotating New Relic transaction')
        return value
    return wrapped_func
