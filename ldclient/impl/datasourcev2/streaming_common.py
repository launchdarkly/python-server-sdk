"""
Shared, transport-agnostic parser for FDv2 streaming messages. Used by both
the sync and async streaming synchronizers.
"""

import json
from dataclasses import dataclass
from time import time
from typing import Optional

from ld_eventsource.actions import Event
from ld_eventsource.errors import HTTPStatusError

from ldclient.impl.datasystem.protocolv2 import (
    DeleteObject,
    Error,
    EventName,
    Goodbye,
    PutObject
)
from ldclient.impl.util import (
    _LD_ENVID_HEADER,
    _LD_FD_FALLBACK_HEADER,
    http_error_message,
    is_http_error_recoverable,
    log
)
from ldclient.interfaces import (
    ChangeSetBuilder,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    IntentCode,
    Selector,
    ServerIntent,
    Update
)


# pylint: disable=too-many-return-statements
def process_message(
    msg: Event, change_set_builder: ChangeSetBuilder, envid: Optional[str]
) -> Optional[Update]:
    """
    Processes a single message from the SSE stream and returns an Update
    object if applicable.

    This function may raise exceptions if the message is malformed or if an
    error occurs while processing the message. The caller should handle these
    exceptions appropriately.
    """
    if msg.event == EventName.HEARTBEAT:
        return None

    if msg.event == EventName.SERVER_INTENT:
        server_intent = ServerIntent.from_dict(json.loads(msg.data))
        change_set_builder.start(server_intent.payload.code)

        if server_intent.payload.code == IntentCode.TRANSFER_NONE:
            change_set_builder.expect_changes()
            return Update(
                state=DataSourceState.VALID,
                environment_id=envid,
            )
        return None

    if msg.event == EventName.PUT_OBJECT:
        put = PutObject.from_dict(json.loads(msg.data))
        change_set_builder.add_put(put.kind, put.key, put.version, put.object)
        return None

    if msg.event == EventName.DELETE_OBJECT:
        delete = DeleteObject.from_dict(json.loads(msg.data))
        change_set_builder.add_delete(delete.kind, delete.key, delete.version)
        return None

    if msg.event == EventName.GOODBYE:
        goodbye = Goodbye.from_dict(json.loads(msg.data))
        log.info("SSE server sent goodbye: %s", goodbye.reason)

        return None

    if msg.event == EventName.ERROR:
        error = Error.from_dict(json.loads(msg.data))
        log.error("Error on %s: %s", error.payload_id, error.reason)

        # The protocol should "reset" any previous change events it has
        # received, but should continue to operate under the assumption the
        # last server intent was in effect.
        #
        # The server may choose to send a new server-intent, at which point
        # we will set that as well.
        change_set_builder.reset()

        return None

    if msg.event == EventName.PAYLOAD_TRANSFERRED:
        selector = Selector.from_dict(json.loads(msg.data))
        change_set = change_set_builder.finish(selector)

        return Update(
            state=DataSourceState.VALID,
            change_set=change_set,
            environment_id=envid,
        )

    log.info("Unexpected event found in stream: %s", msg.event)
    return None


def with_fallback_signal(update: Update, fallback_requested: bool) -> Update:
    """
    Return ``update`` marked with ``fallback_to_fdv1=True`` when the FDv1
    fallback directive has been latched. Returns the update unchanged when the
    directive is not set or the update already carries it.
    """
    if not fallback_requested or update.fallback_to_fdv1:
        return update
    return Update(
        state=update.state,
        change_set=update.change_set,
        error=update.error,
        fallback_to_fdv1=True,
        environment_id=update.environment_id,
    )


@dataclass
class StreamErrorDecision:
    """
    Describes how a streaming synchronizer should react to a connection error.

    The values are computed without any I/O so both the sync and async
    synchronizers share one decision, then apply the side effects themselves:
    they record a failed stream init, set the connection attempt start time to
    ``next_attempt_start``, stop the synchronizer when ``should_stop`` is set,
    and retry the connection when ``should_continue`` is set.
    """
    update: Update
    should_continue: bool
    should_stop: bool
    next_attempt_start: Optional[float]


def classify_stream_error(
    error: Exception, next_retry_delay: float, envid: Optional[str]
) -> StreamErrorDecision:
    """
    Decide how to react to a streaming connection error.

    The caller must first check that the synchronizer is still running; this
    function assumes it is. It returns a decision the caller applies. The caller
    always records a failed stream init before it sets the connection attempt
    start time to the returned ``next_attempt_start``.
    """
    if isinstance(error, json.decoder.JSONDecodeError):
        log.error("Unexpected error on stream connection: %s, will retry", error)
        update = Update(
            state=DataSourceState.INTERRUPTED,
            error=DataSourceErrorInfo(
                DataSourceErrorKind.INVALID_DATA, 0, time(), str(error)
            ),
            fallback_to_fdv1=False,
            environment_id=envid,
        )
        return StreamErrorDecision(
            update=update,
            should_continue=True,
            should_stop=False,
            next_attempt_start=time() + next_retry_delay,
        )

    if isinstance(error, HTTPStatusError):
        next_attempt_start: Optional[float] = time() + next_retry_delay

        error_info = DataSourceErrorInfo(
            DataSourceErrorKind.ERROR_RESPONSE,
            error.status,
            time(),
            str(error),
        )

        if envid is None and error.headers is not None:
            envid = error.headers.get(_LD_ENVID_HEADER)

        if error.headers is not None and error.headers.get(_LD_FD_FALLBACK_HEADER) == 'true':
            update = Update(
                state=DataSourceState.OFF,
                error=error_info,
                fallback_to_fdv1=True,
                environment_id=envid,
            )
            return StreamErrorDecision(
                update=update,
                should_continue=False,
                should_stop=True,
                next_attempt_start=next_attempt_start,
            )

        http_error_message_result = http_error_message(
            error.status, "stream connection"
        )
        is_recoverable = is_http_error_recoverable(error.status)
        update = Update(
            state=(
                DataSourceState.INTERRUPTED
                if is_recoverable
                else DataSourceState.OFF
            ),
            error=error_info,
            fallback_to_fdv1=False,
            environment_id=envid,
        )

        if not is_recoverable:
            log.error(http_error_message_result)
            return StreamErrorDecision(
                update=update,
                should_continue=False,
                should_stop=True,
                next_attempt_start=None,
            )

        log.warning(http_error_message_result)
        return StreamErrorDecision(
            update=update,
            should_continue=True,
            should_stop=False,
            next_attempt_start=next_attempt_start,
        )

    log.warning("Unexpected error on stream connection: %s, will retry", error)
    update = Update(
        state=DataSourceState.INTERRUPTED,
        error=DataSourceErrorInfo(
            DataSourceErrorKind.UNKNOWN, 0, time(), str(error)
        ),
        fallback_to_fdv1=False,
        environment_id=envid,
    )
    # no stacktrace here because, for a typical connection error, it'll
    # just be a lengthy tour of the HTTP client internals
    return StreamErrorDecision(
        update=update,
        should_continue=True,
        should_stop=False,
        next_attempt_start=time() + next_retry_delay,
    )
