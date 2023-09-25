from typing import Callable, Optional, Union, Set, Dict
import time
from datetime import timedelta
from ldclient.evaluation import EvaluationDetail
from ldclient.context import Context
from ldclient.impl.model import FeatureFlag
from threading import Lock
from ldclient.impl.events.types import EventInputMigrationOp
from ldclient.migrations.types import Stage, Operation, Origin


class OpTracker:
    """
    An OpTracker is responsible for managing the collection of measurements
    that which a user might wish to record throughout a migration-assisted
    operation.

    Example measurements include latency, errors, and consistency.

    The OpTracker is not expected to be instantiated directly. Consumers should
    instead call :func:`ldclient.client.LDClient.migration_variation()` and use
    the returned tracker instance.
    """

    def __init__(
        self,
        flag: Optional[FeatureFlag],
        context: Union[Context, dict],
        detail: EvaluationDetail,
        default_stage: Stage
    ):
        self.__flag = flag

        if not isinstance(context, Context):
            context = Context.from_dict(context)
        self.__context = context

        self.__detail = detail
        self.__default_stage = default_stage

        self.__mutex = Lock()

        self.__operation: Optional[Operation] = None
        self.__invoked: Set[Origin] = set()
        self.__consistent: Optional[bool] = None
        self.__consistent_ratio: Optional[int] = None  # TODO: Get this from the flag
        self.__errors: Set[Origin] = set()
        self.__latencies: Dict[Origin, timedelta] = {}

    def operation(self, op: Operation) -> 'OpTracker':
        """
        Sets the migration related operation associated with these tracking
        measurements.

        :param op: The read or write operation symbol.
        """
        if not isinstance(op, Operation):
            return self

        with self.__mutex:
            self.__operation = op
        return self

    def invoked(self, origin: Origin) -> 'OpTracker':
        """
        Allows recording which origins were called during a migration.

        :param origin: Designation for the old or new origin.
        """
        if not isinstance(origin, Origin):
            return self

        with self.__mutex:
            self.__invoked.add(origin)
        return self

    def consistent(self, is_consistent: Callable[[], bool]) -> 'OpTracker':
        """
        Allows recording the results of a consistency check.

        This method accepts a callable which should take no parameters and
        return a single boolean to represent the consistency check results for
        a read operation.

        A callable is provided in case sampling rules do not require
        consistency checking to run. In this case, we can avoid the overhead of
        a function by not using the callable.

        :param is_consistent: closure to return result of comparison check
        """
        # TODO(sampling-ratio): Add sampling checking here
        with self.__mutex:
            self.__consistent = is_consistent()
        return self

    def error(self, origin: Origin) -> 'OpTracker':
        """
        Allows recording whether an error occurred during the operation.

        :param origin: Designation for the old or new origin.
        """
        if not isinstance(origin, Origin):
            return

        with self.__mutex:
            self.__errors.add(origin)
        return self

    def latency(self, origin: Origin, duration: timedelta) -> 'OpTracker':
        """
        Allows tracking the recorded latency for an individual operation.

        :param origin: Designation for the old or new origin.
        :param duration: Duration measurement.
        """
        if not isinstance(origin, Origin):
            return

        with self.__mutex:
            self.__latencies[origin] = duration
        return self

    def build(self) -> Union[EventInputMigrationOp, str]:
        """
        Creates an instance of :class:`ldclient.impl.EventInputMigrationOp()`.
        This event data can be provided to
        :func:`ldclient.client.LDClient.track_migration_op()` to rely this
        metric information upstream to LaunchDarkly services.

        :return: A :class:`ldclient.impl.EventInputMigrationOp()` or a string
            describing the type of failure.
        """
        with self.__mutex:
            if self.__operation is None:
                return "operation not provided"
            if self.__flag is None:
                return "flag not provided"
            if len(self.__invoked) == 0:
                return "no origins were invoked"
            if not self.__context.valid:
                return "provided context was invalid"

            error = self.__check_invoked_consistency()
            if error:
                return error

            # TODO: Inject this time function or something
            timestamp = int(time.time() * 1_000)

            return EventInputMigrationOp(
                timestamp,
                self.__context,
                self.__flag,
                self.__operation,
                self.__default_stage,
                self.__detail,
                self.__invoked.copy(),
                self.__consistent,
                self.__consistent_ratio,
                self.__errors.copy(),
                self.__latencies.copy())

    def __check_invoked_consistency(self) -> Optional[str]:
        for origin in Origin:
            if origin in self.__invoked:
                continue

            if origin in self.__latencies:
                return f"provided latency for origin '{origin.value}' without recording invocation"
            if origin in self.__errors:
                return f"provided error for origin '{origin.value}' without recording invocation"

            # A consistency measurement only makes sense if TWO origins were
            # executed. Otherwise, there is nothing to compare against.
            if self.__consistent is not None and len(self.__invoked) != 2:
                return "provided consistency without recording both invocations"

        return None
