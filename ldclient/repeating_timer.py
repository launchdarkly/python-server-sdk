"""
Internal helper class for repeating tasks.
"""
# currently excluded from documentation - see docs/README.md

from ldclient.impl.repeating_task import RepeatingTask

class RepeatingTimer(RepeatingTask):
    """
    Deprecated internal class, retained until the next major version in case any application code was
    referencing it. This was used in situations where we did not want the callback to execute
    immediately, but to always wait for the interval first, so we are setting both the interval
    parameter and the initial_delay parameter of RepeatingTask to the same value.
    """
    def __init__(self, interval, callable):
        super().init(self, interval, interval, callable)
