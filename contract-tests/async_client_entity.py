import asyncio
import json
import logging
from typing import Optional

import requests
from async_big_segment_store_fixture import AsyncBigSegmentStoreFixture
from async_flag_change_listener import AsyncListenerRegistry
from hook import AsyncPostingHook

from ldclient import Context
from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncBigSegmentsConfig, AsyncConfig
from ldclient.impl.util import Result
from ldclient.migrations import (
    AsyncMigratorBuilder,
    ExecutionOrder,
    Operation,
    Stage
)


class AsyncClientEntity:
    def __init__(self, tag: str, config_params: dict):
        self.log = logging.getLogger(tag)
        self._client: Optional[AsyncLDClient] = None
        self._listeners: Optional[AsyncListenerRegistry] = None
        self._config_params = config_params
        self._tag = tag

    async def start(self):
        """Build the AsyncLDClient and wait for it to initialize."""
        config_params = self._config_params
        opts = {"sdk_key": config_params["credential"]}

        tags = config_params.get('tags', {})
        if tags:
            opts['application'] = {
                'id': tags.get('applicationId', ''),
                'version': tags.get('applicationVersion', ''),
            }

        datasystem_config = config_params.get('dataSystem')
        if datasystem_config is not None:
            raise NotImplementedError("FDv2 (dataSystem) is not yet supported in the async contract-test service")
        elif config_params.get("streaming") is not None:
            streaming = config_params["streaming"]
            if streaming.get("baseUri") is not None:
                opts["stream_uri"] = streaming["baseUri"]
            if streaming.get("filter") is not None:
                opts["payload_filter_key"] = streaming["filter"]
            _set_optional_time_prop(streaming, "initialRetryDelayMs", opts, "initial_reconnect_delay")
        elif config_params.get("polling") is not None:
            opts['stream'] = False
            polling = config_params["polling"]
            if polling.get("baseUri") is not None:
                opts["base_uri"] = polling["baseUri"]
            if polling.get("filter") is not None:
                opts["payload_filter_key"] = polling["filter"]
            _set_optional_time_prop(polling, "pollIntervalMs", opts, "poll_interval")
        else:
            opts['use_ldd'] = True

        if config_params.get("events") is not None:
            events = config_params["events"]
            opts["enable_event_compression"] = events.get("enableGzip", False)
            if events.get("baseUri") is not None:
                opts["events_uri"] = events["baseUri"]
            if events.get("capacity") is not None:
                opts["events_max_pending"] = events["capacity"]
            opts["diagnostic_opt_out"] = not events.get("enableDiagnostics", False)
            opts["all_attributes_private"] = events.get("allAttributesPrivate", False)
            opts["private_attributes"] = events.get("globalPrivateAttributes", {})
            _set_optional_time_prop(events, "flushIntervalMs", opts, "flush_interval")
            opts["omit_anonymous_contexts"] = events.get("omitAnonymousContexts", False)
        else:
            opts["send_events"] = False

        hooks = []
        if config_params.get("hooks") is not None:
            hooks = [
                AsyncPostingHook(h["name"], h["callbackUri"], h.get("data", {}), h.get("errors", {}))
                for h in config_params["hooks"]["hooks"]
            ]

        if config_params.get("bigSegments") is not None:
            big_params = config_params["bigSegments"]
            big_config = {"store": AsyncBigSegmentStoreFixture(big_params["callbackUri"])}
            if big_params.get("userCacheSize") is not None:
                big_config["context_cache_size"] = big_params["userCacheSize"]
            _set_optional_time_prop(big_params, "userCacheTimeMs", big_config, "context_cache_time")
            _set_optional_time_prop(big_params, "statusPollIntervalMs", big_config, "status_poll_interval")
            _set_optional_time_prop(big_params, "staleAfterMs", big_config, "stale_after")
            opts["big_segments"] = AsyncBigSegmentsConfig(**big_config)

        start_wait = config_params.get("startWaitTimeMs") or 5000
        sdk_config = AsyncConfig(**opts)

        self._client = AsyncLDClient(sdk_config)
        # The async client accepts AsyncHook instances only; register the
        # harness's async posting hooks via add_hook() before start().
        for hook in hooks:
            self._client.add_hook(hook)
        await self._client.start(start_wait / 1000.0)
        self._listeners = AsyncListenerRegistry(self._client.flag_tracker)

    async def is_initializing(self) -> bool:
        return await self._client.is_initialized() if self._client else False

    async def evaluate(self, params: dict) -> dict:
        response = {}
        if params.get("detail", False):
            detail = await self._client.variation_detail(
                params["flagKey"], Context.from_dict(params["context"]), params["defaultValue"]
            )
            response["value"] = detail.value
            response["variationIndex"] = detail.variation_index
            response["reason"] = detail.reason
        else:
            response["value"] = await self._client.variation(
                params["flagKey"], Context.from_dict(params["context"]), params["defaultValue"]
            )
        return response

    async def evaluate_all(self, params: dict) -> dict:
        opts = {}
        opts["client_side_only"] = params.get("clientSideOnly", False)
        opts["with_reasons"] = params.get("withReasons", False)
        opts["details_only_for_tracked_flags"] = params.get("detailsOnlyForTrackedFlags", False)
        state = await self._client.all_flags_state(Context.from_dict(params["context"]), **opts)
        return {"state": state.to_json_dict()}

    def track(self, params: dict):
        self._client.track(
            params["eventKey"],
            Context.from_dict(params["context"]),
            params["data"],
            params.get("metricValue", None),
        )

    def identify(self, params: dict):
        self._client.identify(Context.from_dict(params["context"]))

    async def flush(self):
        await self._client.flush()

    def secure_mode_hash(self, params: dict) -> dict:
        return {"result": self._client.secure_mode_hash(Context.from_dict(params["context"]))}

    def context_build(self, params: dict) -> dict:
        if params.get("multi"):
            b = Context.multi_builder()
            for c in params.get("multi"):
                b.add(self._context_build_single(c))
            return self._context_response(b.build())
        return self._context_response(self._context_build_single(params["single"]))

    def _context_build_single(self, params: dict) -> Context:
        b = Context.builder(params["key"])
        if "kind" in params:
            b.kind(params["kind"])
        if "name" in params:
            b.name(params["name"])
        if "anonymous" in params:
            b.anonymous(params["anonymous"])
        if "custom" in params:
            for k, v in params.get("custom").items():
                b.set(k, v)
        if "private" in params:
            for attr in params.get("private"):
                b.private(attr)
        return b.build()

    def context_convert(self, params: dict) -> dict:
        input_str = params["input"]
        try:
            props = json.loads(input_str)
            return self._context_response(Context.from_dict(props))
        except Exception as e:
            return {"error": str(e)}

    def _context_response(self, c: Context) -> dict:
        if c.valid:
            return {"output": c.to_json_string()}
        return {"error": c.error}

    async def get_big_segment_store_status(self) -> dict:
        status = await self._client.big_segment_store_status_provider.get_status()
        return {"available": status.available, "stale": status.stale}

    async def migration_variation(self, params: dict) -> dict:
        stage, _ = await self._client.migration_variation(
            params["key"], Context.from_dict(params["context"]), Stage.from_str(params["defaultStage"])
        )
        return {'result': stage.value}

    async def migration_operation(self, params: dict) -> dict:
        # Exercises the real AsyncMigratorBuilder/AsyncMigrator abstraction. The
        # user read/write callbacks are async functions that run the blocking
        # requests.post off the event loop via asyncio.to_thread.
        if params["readExecutionOrder"] == "concurrent":
            params["readExecutionOrder"] = "parallel"

        def callback(endpoint):
            async def fn(payload) -> Result:
                def do_post() -> Result:
                    response = requests.post(endpoint, data=payload)
                    if response.status_code == 200:
                        return Result.success(response.text)
                    return Result.fail(f"Request failed with status code {response.status_code}")

                return await asyncio.to_thread(do_post)

            return fn

        builder = AsyncMigratorBuilder(self._client)
        builder.read_execution_order(ExecutionOrder.from_str(params["readExecutionOrder"]))
        builder.track_latency(params["trackLatency"])
        builder.track_errors(params["trackErrors"])

        comparison = (lambda lhs, rhs: lhs == rhs) if params["trackConsistency"] else None
        builder.read(callback(params["oldEndpoint"]), callback(params["newEndpoint"]), comparison)
        builder.write(callback(params["oldEndpoint"]), callback(params["newEndpoint"]))

        migrator = builder.build()
        if isinstance(migrator, str):
            return {"result": migrator}

        key = params["key"]
        context = Context.from_dict(params["context"])
        default_stage = Stage.from_str(params["defaultStage"])
        payload = params["payload"]

        if params["operation"] == Operation.READ.value:
            result = await migrator.read(key, context, default_stage, payload)
            return {"result": result.value if result.is_success() else result.error}

        write_result = await migrator.write(key, context, default_stage, payload)
        authoritative = write_result.authoritative
        return {"result": authoritative.value if authoritative.is_success() else authoritative.error}

    async def register_flag_change_listener(self, params: dict):
        await self._listeners.register_flag_change_listener(
            listener_id=params['listenerId'],
            callback_uri=params['callbackUri'],
        )

    async def register_flag_value_change_listener(self, params: dict):
        await self._listeners.register_flag_value_change_listener(
            listener_id=params["listenerId"],
            flag_key=params["flagKey"],
            context=Context.from_dict(params["context"]),
            callback_uri=params["callbackUri"],
        )

    async def unregister_listener(self, params: dict) -> bool:
        return await self._listeners.unregister(params['listenerId'])

    async def close(self):
        if self._listeners is not None:
            await self._listeners.close_all()
        if self._client is not None:
            await self._client.close()
        self.log.info('Test ended')


def _set_optional_time_prop(params_in: dict, name_in: str, params_out: dict, name_out: str):
    if params_in.get(name_in) is not None:
        params_out[name_out] = params_in[name_in] / 1000.0
