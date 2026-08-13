"""
Shared, transport-agnostic parsers that convert polling payloads into
ChangeSets. Used by both the sync and async polling requesters.
"""

from dataclasses import dataclass
from enum import Enum
from time import time
from typing import Mapping, Tuple

from ldclient.impl.datasystem.protocolv2 import (
    DeleteObject,
    EventName,
    PutObject
)
from ldclient.impl.util import (
    _LD_ENVID_HEADER,
    _LD_FD_FALLBACK_HEADER,
    UnsuccessfulResponseException,
    _Fail,
    _Result,
    _Success,
    http_error_message,
    is_http_error_recoverable,
    log
)
from ldclient.interfaces import (
    Basis,
    BasisResult,
    ChangeSet,
    ChangeSetBuilder,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    IntentCode,
    ObjectKind,
    Selector,
    ServerIntent,
    Update
)


# pylint: disable=too-many-branches,too-many-return-statements
def polling_payload_to_changeset(data: dict) -> _Result[ChangeSet, str]:
    """
    Converts a polling payload into a ChangeSet.
    """
    if "events" not in data or not isinstance(data["events"], list):
        return _Fail(error="Invalid payload: 'events' key is missing or not a list")

    builder = ChangeSetBuilder()

    for event in data["events"]:
        if not isinstance(event, dict):
            return _Fail(error="Invalid payload: 'events' must be a list of objects")

        if "event" not in event:
            continue

        if event["event"] == EventName.SERVER_INTENT:
            try:
                server_intent = ServerIntent.from_dict(event["data"])
            except ValueError as err:
                return _Fail(error="Invalid JSON in server intent", exception=err)

            if server_intent.payload.code == IntentCode.TRANSFER_NONE:
                return _Success(ChangeSetBuilder.no_changes())

            builder.start(server_intent.payload.code)
        elif event["event"] == EventName.PUT_OBJECT:
            try:
                put = PutObject.from_dict(event["data"])
            except ValueError as err:
                return _Fail(error="Invalid JSON in put object", exception=err)

            builder.add_put(put.kind, put.key, put.version, put.object)
        elif event["event"] == EventName.DELETE_OBJECT:
            try:
                delete_object = DeleteObject.from_dict(event["data"])
            except ValueError as err:
                return _Fail(error="Invalid JSON in delete object", exception=err)

            builder.add_delete(
                delete_object.kind, delete_object.key, delete_object.version
            )
        elif event["event"] == EventName.PAYLOAD_TRANSFERRED:
            try:
                selector = Selector.from_dict(event["data"])
                changeset = builder.finish(selector)

                return _Success(value=changeset)
            except ValueError as err:
                return _Fail(
                    error="Invalid JSON in payload transferred object", exception=err
                )

    return _Fail(error="didn't receive any known protocol events in polling payload")


# pylint: disable=too-many-branches,too-many-return-statements
def fdv1_polling_payload_to_changeset(data: dict) -> _Result[ChangeSet, str]:
    """
    Converts a fdv1 polling payload into a ChangeSet.
    """
    builder = ChangeSetBuilder()
    builder.start(IntentCode.TRANSFER_FULL)
    selector = Selector.no_selector()

    # FDv1 uses "flags" instead of "features", so we need to map accordingly
    # Map FDv1 JSON keys to ObjectKind enum values
    kind_mappings = [
        (ObjectKind.FLAG, "flags"),
        (ObjectKind.SEGMENT, "segments")
    ]

    for kind, fdv1_key in kind_mappings:
        kind_data = data.get(fdv1_key)
        if kind_data is None:
            continue
        if not isinstance(kind_data, dict):
            return _Fail(error=f"Invalid format: {fdv1_key} is not a dictionary")

        for key in kind_data:
            flag_or_segment = kind_data.get(key)
            if flag_or_segment is None or not isinstance(flag_or_segment, dict):
                return _Fail(error=f"Invalid format: {key} is not a dictionary")

            version = flag_or_segment.get('version')
            if version is None:
                return _Fail(error=f"Invalid format: {key} does not have a version set")

            builder.add_put(kind, key, version, flag_or_segment)

    return _Success(builder.finish(selector))


def polling_result_to_basis(
    result: _Result[Tuple[ChangeSet, Mapping], str]
) -> BasisResult:
    """
    Convert a requester fetch result into a Basis, or a failure. Used by both
    the sync and async polling initializers after they fetch a payload.
    """
    if isinstance(result, _Fail):
        if isinstance(result.exception, UnsuccessfulResponseException):
            status_code = result.exception.status
            http_error_message_result = http_error_message(
                status_code, "polling request"
            )
            if is_http_error_recoverable(status_code):
                log.warning(http_error_message_result)

            # Forward any response headers so callers (e.g. FDv2 datasystem)
            # can read the X-LD-FD-Fallback directive even on error.
            return _Fail(
                error=http_error_message_result,
                exception=result.exception,
                headers=result.headers,
            )

        return _Fail(
            error=result.error or "Failed to request payload",
            exception=result.exception,
            headers=result.headers,
        )

    (change_set, headers) = result.value

    env_id = headers.get(_LD_ENVID_HEADER)
    if not isinstance(env_id, str):
        env_id = None

    basis = Basis(
        change_set=change_set,
        persist=change_set.selector.is_defined(),
        environment_id=env_id,
        fallback_to_fdv1=headers.get(_LD_FD_FALLBACK_HEADER) == 'true',
    )

    return _Success(value=basis)


class PollAction(Enum):
    """
    How a polling synchronizer's loop should proceed after a fetch.

    ``BREAK`` stops the loop. ``WAIT_CONTINUE`` waits the poll interval and then
    starts the next iteration. ``WAIT_BREAK_IF_SET`` waits the poll interval and
    stops only when the interrupt event fired during the wait.
    """
    BREAK = "break"
    WAIT_CONTINUE = "wait_continue"
    WAIT_BREAK_IF_SET = "wait_break_if_set"


@dataclass
class PollDecision:
    """
    The update a polling synchronizer emits after a fetch, and how its loop
    should proceed.
    """
    update: Update
    control: PollAction


def map_polling_result(
    result: _Result[Tuple[ChangeSet, Mapping], str]
) -> PollDecision:
    """
    Convert a requester fetch result into the update a polling synchronizer
    emits and the control signal for its loop. Used by both the sync and async
    polling synchronizers.
    """
    if isinstance(result, _Fail):
        fallback = None
        envid = None

        if result.headers is not None:
            fallback = result.headers.get(_LD_FD_FALLBACK_HEADER) == 'true'
            envid = result.headers.get(_LD_ENVID_HEADER)

        if isinstance(result.exception, UnsuccessfulResponseException):
            error_info = DataSourceErrorInfo(
                kind=DataSourceErrorKind.ERROR_RESPONSE,
                status_code=result.exception.status,
                time=time(),
                message=http_error_message(
                    result.exception.status, "polling request"
                ),
            )

            if fallback:
                return PollDecision(
                    Update(
                        state=DataSourceState.OFF,
                        error=error_info,
                        fallback_to_fdv1=True,
                        environment_id=envid,
                    ),
                    PollAction.BREAK,
                )

            status_code = result.exception.status
            if is_http_error_recoverable(status_code):
                return PollDecision(
                    Update(
                        state=DataSourceState.INTERRUPTED,
                        error=error_info,
                        environment_id=envid,
                    ),
                    PollAction.WAIT_CONTINUE,
                )

            return PollDecision(
                Update(
                    state=DataSourceState.OFF,
                    error=error_info,
                    environment_id=envid,
                ),
                PollAction.BREAK,
            )

        error_info = DataSourceErrorInfo(
            kind=DataSourceErrorKind.NETWORK_ERROR,
            time=time(),
            status_code=0,
            message=result.error,
        )

        # Even a non-HTTP error (e.g. malformed JSON) can carry the fallback
        # header. If so, halt rather than retrying the FDv2 endpoint.
        if fallback:
            return PollDecision(
                Update(
                    state=DataSourceState.OFF,
                    error=error_info,
                    fallback_to_fdv1=True,
                    environment_id=envid,
                ),
                PollAction.BREAK,
            )

        return PollDecision(
            Update(
                state=DataSourceState.INTERRUPTED,
                error=error_info,
                environment_id=envid,
            ),
            PollAction.WAIT_BREAK_IF_SET,
        )

    (change_set, headers) = result.value
    return PollDecision(
        Update(
            state=DataSourceState.VALID,
            change_set=change_set,
            environment_id=headers.get(_LD_ENVID_HEADER),
            fallback_to_fdv1=headers.get(_LD_FD_FALLBACK_HEADER) == 'true',
        ),
        PollAction.WAIT_BREAK_IF_SET,
    )
