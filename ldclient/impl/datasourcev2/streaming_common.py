"""
Shared, transport-agnostic parser for FDv2 streaming messages. Used by both
the sync and async streaming synchronizers.
"""

import json
from typing import Optional

from ld_eventsource.actions import Event

from ldclient.impl.datasystem.protocolv2 import (
    DeleteObject,
    Error,
    EventName,
    Goodbye,
    PutObject
)
from ldclient.impl.util import log
from ldclient.interfaces import (
    ChangeSetBuilder,
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
