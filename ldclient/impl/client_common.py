"""
Genuinely I/O-free, await-free helpers shared by the sync :class:`ldclient.client.LDClient`
and async :class:`ldclient.async_client.AsyncLDClient`.

These functions contain no awaits and touch no store/network/event I/O, so they
can live in a single module imported by both clients. Anything that reads the
store, sends events, or runs hook stages is I/O-adjacent and is hand-duplicated
across the two client classes instead (differing only in ``async``/``await``).
"""

import hashlib
import hmac
from typing import List

from ldclient.config import Config, SdkIdentityConfig
from ldclient.context import Context
from ldclient.hook import Hook
from ldclient.impl.util import log
from ldclient.plugin import (
    ApplicationMetadata,
    EnvironmentMetadata,
    SdkMetadata
)
from ldclient.version import VERSION


def get_environment_metadata(config: SdkIdentityConfig, sdk_name: str) -> EnvironmentMetadata:
    sdk_metadata = SdkMetadata(
        name=sdk_name,
        version=VERSION,
        wrapper_name=config.wrapper_name,
        wrapper_version=config.wrapper_version
    )

    application_metadata = None
    if config.application:
        application_metadata = ApplicationMetadata(
            id=config.application.get('id'),
            version=config.application.get('version'),
        )

    return EnvironmentMetadata(
        sdk=sdk_metadata,
        application=application_metadata,
        sdk_key=config.sdk_key
    )


def get_plugin_hooks(config: Config, environment_metadata: EnvironmentMetadata) -> List[Hook]:
    hooks = []
    for plugin in config.plugins:
        try:
            hooks.extend(plugin.get_hooks(environment_metadata))
        except Exception as e:
            log.error("Error getting hooks from plugin %s: %s", plugin.metadata.name, e)
    return hooks


def secure_mode_hash(config: SdkIdentityConfig, context: Context) -> str:
    """Computes the secure-mode HMAC for a context, or an empty string for an
    invalid context. Pure: depends only on the SDK key and the context's
    fully-qualified key."""
    if not context.valid:
        log.warning("Context was invalid for secure_mode_hash (%s); returning empty hash" % context.error)
        return ""
    return hmac.new(str(config.sdk_key).encode(), context.fully_qualified_key.encode(), hashlib.sha256).hexdigest()
