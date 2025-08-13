"""
This module contains the protocol definitions and data types for the
LaunchDarkly data system version 2 (FDv2).
"""

from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Protocol

from ldclient.impl.util import Result


class EventName(str, Enum):
    """
    EventName represents the name of an event that can be sent by the server for FDv2.
    """

    PUT_OBJECT = "put-object"
    """
    Specifies that an object should be added to the data set with upsert semantics.
    """

    DELETE_OBJECT = "delete-object"
    """
    Specifies that an object should be removed from the data set.
    """

    SERVER_INTENT = "server-intent"
    """
    Specifies the server's intent.
    """

    PAYLOAD_TRANSFERRED = "payload-transferred"
    """
    Specifies that that all data required to bring the existing data set to
    a new version has been transferred.
    """

    HEARTBEAT = "heart-beat"
    """
    Keeps the connection alive.
    """

    GOODBYE = "goodbye"
    """
    Specifies that the server is about to close the connection.
    """

    ERROR = "error"
    """
    Specifies that an error occurred while serving the connection.
    """


class IntentCode(str, Enum):
    """
    IntentCode represents the various intents that can be sent by the server.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    TRANSFER_FULL = "xfer-full"
    """
    The server intends to send a full data set.
    """
    TRANSFER_CHANGES = "xfer-changes"
    """
    The server intends to send only the necessary changes to bring an existing
    data set up-to-date.
    """

    TRANSFER_NONE = "none"
    """
    The server intends to send no data (payload is up to date).
    """


@dataclass(frozen=True)
class Payload:
    """
    Payload represents a payload delivered in a streaming response.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    id: str
    target: int
    code: IntentCode
    reason: str

    def to_dict(self) -> dict:
        """
        Serializes the Payload to a JSON-compatible dictionary.
        """
        return {
            "id": self.id,
            "target": self.target,
            "intentCode": self.code.value,
            "reason": self.reason,
        }

    @staticmethod
    def from_dict(data: dict) -> "Payload":
        """
        Create a Payload from a dictionary representation.
        """
        intent_code = data.get("intentCode")

        if intent_code is None or not isinstance(intent_code, str):
            raise ValueError(
                "Invalid data for Payload: 'intentCode' key is missing or not a string"
            )

        return Payload(
            id=data.get("id", ""),
            target=data.get("target", 0),
            code=IntentCode(intent_code),
            reason=data.get("reason", ""),
        )


@dataclass(frozen=True)
class ServerIntent:
    """
    ServerIntent represents the type of change associated with the payload
    (e.g., transfer full, transfer changes, etc.)
    """

    payload: Payload

    def to_dict(self) -> dict:
        """
        Serializes the ServerIntent to a JSON-compatible dictionary.
        """
        return {
            "payloads": [self.payload.to_dict()],
        }

    @staticmethod
    def from_dict(data: dict) -> "ServerIntent":
        """
        Create a ServerIntent from a dictionary representation.
        """
        if "payloads" not in data or not isinstance(data["payloads"], list):
            raise ValueError(
                "Invalid data for ServerIntent: 'payloads' key is missing or not a list"
            )
        if len(data["payloads"]) != 1:
            raise ValueError(
                "Invalid data for ServerIntent: expected exactly one payload"
            )

        payload = data["payloads"][0]
        if not isinstance(payload, dict):
            raise ValueError("Invalid payload in ServerIntent: expected a dictionary")

        return ServerIntent(payload=Payload.from_dict(payload))


class ObjectKind(str, Enum):
    """
    ObjectKind represents the kind of object.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    FLAG = "flag"
    SEGMENT = "segment"


@dataclass(frozen=True)
class DeleteObject:
    """
    Specifies the deletion of a particular object.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    version: int
    kind: ObjectKind
    key: str

    def name(self) -> str:
        """
        Event method.
        """
        return EventName.DELETE_OBJECT

    def to_dict(self) -> dict:
        """
        Serializes the DeleteObject to a JSON-compatible dictionary.
        """
        return {
            "version": self.version,
            "kind": self.kind.value,
            "key": self.key,
        }

    @staticmethod
    def from_dict(data: dict) -> "DeleteObject":
        """
        Deserializes a DeleteObject from a JSON-compatible dictionary.
        """
        version = data.get("version")
        kind = data.get("kind")
        key = data.get("key")

        if version is None or kind is None or key is None:
            raise ValueError("Missing required fields in DeleteObject JSON.")

        return DeleteObject(version=version, kind=ObjectKind(kind), key=key)


@dataclass(frozen=True)
class PutObject:
    """
    Specifies the addition of a particular object with upsert semantics.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    version: int
    kind: ObjectKind
    key: str
    object: dict

    def name(self) -> str:
        """
        Event method.
        """
        return EventName.PUT_OBJECT

    def to_dict(self) -> dict:
        """
        Serializes the PutObject to a JSON-compatible dictionary.
        """
        return {
            "version": self.version,
            "kind": self.kind.value,
            "key": self.key,
            "object": self.object,
        }

    @staticmethod
    def from_dict(data: dict) -> "PutObject":
        """
        Deserializes a PutObject from a JSON-compatible dictionary.
        """
        version = data.get("version")
        kind = data.get("kind")
        key = data.get("key")
        object_data = data.get("object")

        if version is None or kind is None or key is None or object_data is None:
            raise ValueError("Missing required fields in PutObject JSON.")

        return PutObject(
            version=version, kind=ObjectKind(kind), key=key, object=object_data
        )


@dataclass(frozen=True)
class Goodbye:
    """
    Goodbye represents a goodbye event.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    reason: str
    silent: bool
    catastrophe: bool

    def to_dict(self) -> dict:
        """
        Serializes the Goodbye to a JSON-compatible dictionary.
        """
        return {
            "reason": self.reason,
            "silent": self.silent,
            "catastrophe": self.catastrophe,
        }

    @staticmethod
    def from_dict(data: dict) -> "Goodbye":
        """
        Deserializes a Goodbye event from a JSON-compatible dictionary.
        """
        reason = data.get("reason")
        silent = data.get("silent")
        catastrophe = data.get("catastrophe")

        if reason is None or silent is None or catastrophe is None:
            raise ValueError("Missing required fields in Goodbye JSON.")

        return Goodbye(reason=reason, silent=silent, catastrophe=catastrophe)


@dataclass(frozen=True)
class Error:
    """
    Error represents an error event.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    payload_id: str
    reason: str

    def to_dict(self) -> dict:
        """
        Serializes the Error to a JSON-compatible dictionary.
        """
        return {
            "payloadId": self.payload_id,
            "reason": self.reason,
        }

    @staticmethod
    def from_dict(data: dict) -> "Error":
        """
        Deserializes an Error from a JSON-compatible dictionary.
        """
        payload_id = data.get("payloadId")
        reason = data.get("reason")

        if payload_id is None or reason is None:
            raise ValueError("Missing required fields in Error JSON.")

        return Error(payload_id=payload_id, reason=reason)


@dataclass(frozen=True)
class Selector:
    """
    Selector represents a particular snapshot of data.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    state: str = ""
    version: int = 0

    @staticmethod
    def no_selector() -> "Selector":
        """
        Returns an empty Selector.
        """
        return Selector()

    def is_defined(self) -> bool:
        """
        Returns True if the Selector has a value.
        """
        return self != Selector.no_selector()

    def name(self) -> str:
        """
        Event method.
        """
        return EventName.PAYLOAD_TRANSFERRED

    @staticmethod
    def new_selector(state: str, version: int) -> "Selector":
        """
        Creates a new Selector from a state string and version.
        """
        return Selector(state=state, version=version)

    def to_dict(self) -> dict:
        """
        Serializes the Selector to a JSON-compatible dictionary.
        """
        return {"state": self.state, "version": self.version}

    @staticmethod
    def from_dict(data: dict) -> "Selector":
        """
        Deserializes a Selector from a JSON-compatible dictionary.
        """
        state = data.get("state")
        version = data.get("version")

        if state is None or version is None:
            raise ValueError("Missing required fields in Selector JSON.")

        return Selector(state=state, version=version)


class ChangeType(Enum):
    """
    ChangeType specifies if an object is being upserted or deleted.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    PUT = "put"
    """
    Represents an object being upserted.
    """

    DELETE = "delete"
    """
    Represents an object being deleted.
    """


@dataclass(frozen=True)
class Change:
    """
    Change represents a change to a piece of data, such as an update or deletion.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    action: ChangeType
    kind: ObjectKind
    key: str
    version: int
    object: Any = (
        None  # TODO(fdv2): At some point, we should define a better type for this.
    )


@dataclass(frozen=True)
class ChangeSet:
    """
    ChangeSet represents a list of changes to be applied.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    intent_code: IntentCode
    changes: List[Change]
    selector: Optional[Selector]


@dataclass(frozen=True)
class Basis:
    """
    Basis represents the initial payload of data that a data source can
    provide. Initializers provide this via fetch, whereas Synchronizers provide
    it asynchronously.
    """

    change_set: ChangeSet
    persist: bool
    environment_id: Optional[str] = None


class Synchronizer(Protocol):
    """
    Represents a component capable of obtaining a Basis and subsequent delta
    updates asynchronously.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the name of the initializer."""
        raise NotImplementedError

    # TODO(fdv2): Need sync method

    def close(self):
        """
        Close the synchronizer, releasing any resources it holds.
        """


class Initializer(Protocol):
    """
    Represents a component capable of obtaining a Basis via a synchronous call.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the name of the initializer."""
        raise NotImplementedError

    @abstractmethod
    def fetch(self) -> Result:
        """
        Fetch returns a Basis, or an error if the Basis could not be retrieved.
        """
        raise NotImplementedError


class ChangeSetBuilder:
    """
    ChangeSetBuilder is a helper for constructing a ChangeSet.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.
    """

    def __init__(self):
        """
        Initializes a new ChangeSetBuilder.
        """
        self.intent = None
        self.changes = []

    @staticmethod
    def no_changes() -> "ChangeSet":
        """
        Represents an intent that the current data is up-to-date and doesn't
        require changes.
        """
        return ChangeSet(
            intent_code=IntentCode.TRANSFER_NONE, selector=None, changes=[]
        )

    @staticmethod
    def empty(selector) -> "ChangeSet":
        """
        Returns an empty ChangeSet, which is useful for initializing a client
        without data or for clearing out all existing data.
        """
        return ChangeSet(
            intent_code=IntentCode.TRANSFER_FULL, selector=selector, changes=[]
        )

    def start(self, intent: IntentCode):
        """
        Begins a new change set with a given intent.
        """
        self.intent = intent
        self.changes = []

    def expect_changes(self):
        """
        Ensures that the current ChangeSetBuilder is prepared to handle changes.

        If a data source's initial connection reflects an updated status, we
        need to keep the provided server intent. This allows subsequent changes
        to come down the line without an explicit server intent.

        However, to maintain logical consistency, we need to ensure that the intent
        is set to IntentTransferChanges.
        """
        if self.intent is None:
            raise ValueError("changeset: cannot expect changes without a server-intent")

        if self.intent != IntentCode.TRANSFER_NONE:
            return

        self.intent = IntentCode.TRANSFER_CHANGES

    def reset(self):
        """
        Clears any existing changes while preserving the current intent.
        """
        self.changes = []

    def finish(self, selector) -> ChangeSet:
        """
        Identifies a changeset with a selector and returns the completed
        changeset. Clears any existing changes while preserving the current
        intent, so the builder can be reused.
        """
        if self.intent is None:
            raise ValueError("changeset: cannot complete without a server-intent")

        changeset = ChangeSet(
            intent_code=self.intent, selector=selector, changes=self.changes
        )
        self.changes = []

        # Once a full transfer has been processed, all future changes should be
        # assumed to be changes. Flag delivery can override this behavior by
        # sending a new server intent to any connected stream.
        if self.intent == IntentCode.TRANSFER_FULL:
            self.intent = IntentCode.TRANSFER_CHANGES

        return changeset

    def add_put(self, kind, key, version, obj):
        """
        Adds a new object to the changeset.
        """
        self.changes.append(
            Change(
                action=ChangeType.PUT, kind=kind, key=key, version=version, object=obj
            )
        )

    def add_delete(self, kind, key, version):
        """
        Adds a deletion to the changeset.
        """
        self.changes.append(
            Change(action=ChangeType.DELETE, kind=kind, key=key, version=version)
        )
