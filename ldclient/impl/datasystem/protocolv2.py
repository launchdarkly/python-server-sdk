"""
This module contains the protocol definitions and data types for the
LaunchDarkly data system version 2 (FDv2).
"""

from dataclasses import dataclass

from ldclient.interfaces import EventName, ObjectKind


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
