"""
Shared, transport-agnostic parsers that convert polling payloads into
ChangeSets. Used by both the sync and async polling requesters.
"""

from ldclient.impl.datasystem.protocolv2 import (
    DeleteObject,
    EventName,
    PutObject
)
from ldclient.impl.util import _Fail, _Result, _Success
from ldclient.interfaces import (
    ChangeSet,
    ChangeSetBuilder,
    IntentCode,
    ObjectKind,
    Selector,
    ServerIntent
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
