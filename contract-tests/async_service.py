import asyncio
import json
import logging
import os
import sys
from logging.config import dictConfig
from typing import Any, Callable, Dict, Optional

# Import ldclient from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import aiohttp.web  # noqa: E402
import requests  # noqa: E402

from ldclient import Context  # noqa: E402
from ldclient.async_client import AsyncLDClient  # noqa: E402
from ldclient.async_config import (  # noqa: E402
    AsyncBigSegmentsConfig,
    AsyncConfig
)
from ldclient.impl.util import Result  # noqa: E402
from ldclient.interfaces import AsyncBigSegmentStore  # noqa: E402
from ldclient.migrations import (  # noqa: E402
    AsyncMigratorBuilder,
    ExecutionOrder,
    Operation,
    Stage
)

default_port = 8000

dictConfig(
    {
        'version': 1,
        'formatters': {
            'default': {
                'format': '[%(asctime)s] [%(name)s] %(levelname)s: %(message)s',
            }
        },
        'handlers': {'console': {'class': 'logging.StreamHandler', 'formatter': 'default'}},
        'root': {'level': 'INFO', 'handlers': ['console']},
        'loggers': {
            'ldclient': {
                'level': 'INFO',
            },
        },
    }
)

global_log = logging.getLogger('async_testservice')

client_counter = 0
clients: Dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Async listener registry
# ---------------------------------------------------------------------------

class AsyncListenerRegistry:
    """Manages flag change listener registrations for a single AsyncLDClient entity."""

    def __init__(self, tracker):
        self._tracker = tracker
        self._lock = asyncio.Lock()
        # Maps listener_id -> underlying listener (sync callable or AsyncFlagValueChangeListener)
        self._listeners: Dict[str, Any] = {}

    async def register_flag_change_listener(self, listener_id: str, callback_uri: str):
        import aiohttp

        async def on_flag_change(flag_change):
            payload = {
                'listenerId': listener_id,
                'flagKey': flag_change.key,
            }
            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(callback_uri, json=payload)
            except Exception as e:
                global_log.warning('Failed to post flag change notification: %s', e)

        # AsyncFlagTrackerImpl.add_listener takes a sync callable.
        # We schedule the coroutine via run_coroutine_threadsafe so the sync wrapper remains non-blocking.
        loop = asyncio.get_running_loop()

        def sync_wrapper(flag_change):
            asyncio.run_coroutine_threadsafe(on_flag_change(flag_change), loop)

        async with self._lock:
            if listener_id in self._listeners:
                self._tracker.remove_listener(self._listeners[listener_id])
            self._tracker.add_listener(sync_wrapper)
            self._listeners[listener_id] = sync_wrapper

    async def register_flag_value_change_listener(
        self,
        listener_id: str,
        flag_key: str,
        context: Context,
        callback_uri: str,
    ):
        import aiohttp

        loop = asyncio.get_running_loop()

        def on_value_change(change):
            payload = {
                'listenerId': listener_id,
                'flagKey': change.key,
                'oldValue': change.old_value,
                'newValue': change.new_value,
            }

            async def _post():
                try:
                    async with aiohttp.ClientSession() as session:
                        await session.post(callback_uri, json=payload)
                except Exception as e:
                    global_log.warning('Failed to post flag value change notification: %s', e)

            asyncio.run_coroutine_threadsafe(_post(), loop)

        async with self._lock:
            if listener_id in self._listeners:
                old = self._listeners[listener_id]
                self._tracker.remove_listener(old)

            value_listener = await self._tracker.add_flag_value_change_listener(
                flag_key, context, on_value_change
            )
            self._listeners[listener_id] = value_listener

    async def unregister(self, listener_id: str) -> bool:
        async with self._lock:
            listener = self._listeners.pop(listener_id, None)
            if listener is None:
                return False
            self._tracker.remove_listener(listener)
            return True

    async def close_all(self):
        async with self._lock:
            for listener in self._listeners.values():
                self._tracker.remove_listener(listener)
            self._listeners.clear()


# ---------------------------------------------------------------------------
# Async client entity
# ---------------------------------------------------------------------------

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
            from ldclient.datasystem import custom
            from ldclient.impl.datasourcev2.async_polling import \
                AsyncFallbackToFDv1PollingDataSourceBuilder as \
                fdv1_fallback_ds_builder
            from ldclient.impl.datasourcev2.async_polling import \
                AsyncPollingDataSourceBuilder as polling_ds_builder
            from ldclient.impl.datasourcev2.async_streaming import \
                AsyncStreamingDataSourceBuilder as streaming_ds_builder
            datasystem = custom()

            init_configs = datasystem_config.get('initializers')
            if init_configs is not None:
                initializers = []
                for init_config in init_configs:
                    polling = init_config.get('polling')
                    if polling is not None:
                        polling_builder = polling_ds_builder()
                        _set_optional_value(polling, "baseUri", polling_builder.base_uri)
                        _set_optional_time(polling, "pollIntervalMs", polling_builder.poll_interval)
                        initializers.append(polling_builder)
                datasystem.initializers(initializers)

            sync_configs = datasystem_config.get('synchronizers')
            if sync_configs is not None:
                sync_builders = []
                for sync_config in sync_configs:
                    streaming = sync_config.get('streaming')
                    if streaming is not None:
                        builder = streaming_ds_builder()
                        _set_optional_value(streaming, "baseUri", builder.base_uri)
                        _set_optional_time(streaming, "initialRetryDelayMs", builder.initial_reconnect_delay)
                        sync_builders.append(builder)
                    elif sync_config.get('polling') is not None:
                        polling = sync_config.get('polling')
                        builder = polling_ds_builder()
                        _set_optional_value(polling, "baseUri", builder.base_uri)
                        _set_optional_time(polling, "pollIntervalMs", builder.poll_interval)
                        sync_builders.append(builder)
                if sync_builders:
                    datasystem.synchronizers(*sync_builders)

            fdv1_fallback_config = datasystem_config.get('fdv1Fallback')
            if fdv1_fallback_config is not None:
                fallback_builder = fdv1_fallback_ds_builder()
                _set_optional_value(fdv1_fallback_config, "baseUri", fallback_builder.base_uri)
                _set_optional_time(fdv1_fallback_config, "pollIntervalMs", fallback_builder.poll_interval)
                datasystem.fdv1_compatible_synchronizer(fallback_builder)

            if datasystem_config.get("payloadFilter") is not None:
                opts["payload_filter_key"] = datasystem_config["payloadFilter"]

            store_config = datasystem_config.get("store")
            if store_config is not None:
                persistent_store_config = store_config.get("persistentDataStore")
                if persistent_store_config is not None:
                    from ldclient.interfaces import DataStoreMode
                    store = _create_persistent_store(persistent_store_config)
                    store_mode_value = datasystem_config.get("storeMode", 0)
                    store_mode = DataStoreMode.READ_WRITE if store_mode_value == 1 else DataStoreMode.READ_ONLY
                    datasystem.data_store(store, store_mode)

            opts["datasystem_config"] = datasystem.build()

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
            from hook import AsyncPostingHook
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

        if config_params.get("persistentDataStore") is not None:
            opts["feature_store"] = _create_persistent_store(config_params["persistentDataStore"])

        start_wait = config_params.get("startWaitTimeMs") or 5000
        sdk_config = AsyncConfig(**opts)

        self._client = AsyncLDClient(sdk_config)
        # The async client accepts AsyncHook instances only; register the
        # harness's async posting hooks via add_hook() before start().
        for hook in hooks:
            self._client.add_hook(hook)
        await self._client.start(start_wait / 1000.0)
        self._listeners = AsyncListenerRegistry(self._client.flag_tracker)

    def is_initializing(self) -> bool:
        return self._client.is_initialized() if self._client else False

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
        status = self._client.big_segment_store_status_provider.status
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
                    return Result.error(f"Request failed with status code {response.status_code}")

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
            raise Exception(f"failed to build migrator: {migrator}")

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


# ---------------------------------------------------------------------------
# Async big segment store fixture
# ---------------------------------------------------------------------------

class AsyncBigSegmentStoreFixture(AsyncBigSegmentStore):
    """AsyncBigSegmentStore implementation that calls back to the test harness."""

    def __init__(self, callback_uri: str):
        self._callback_uri = callback_uri

    async def get_metadata(self):
        from ldclient.interfaces import BigSegmentStoreMetadata
        resp_data = await self._post_callback('/getMetadata', None)
        return BigSegmentStoreMetadata(resp_data.get("lastUpToDate"))

    async def get_membership(self, context_hash: str):
        resp_data = await self._post_callback('/getMembership', {'contextHash': context_hash})
        return resp_data.get("values")

    async def _post_callback(self, path: str, params) -> dict:
        import aiohttp
        url = self._callback_uri + path
        async with aiohttp.ClientSession() as session:
            if params is None:
                async with session.post(url) as resp:
                    if resp.status != 200:
                        raise Exception("HTTP error %d from callback to %s" % (resp.status, url))
                    return await resp.json()
            else:
                async with session.post(url, json=params) as resp:
                    if resp.status != 200:
                        raise Exception("HTTP error %d from callback to %s" % (resp.status, url))
                    return await resp.json()

    async def stop(self):
        pass


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

async def handle_status(request: aiohttp.web.Request) -> aiohttp.web.Response:
    body = {
        'capabilities': [
            'server-side',
            'server-side-polling',
            'all-flags-with-reasons',
            'all-flags-client-side-only',
            'all-flags-details-only-for-tracked-flags',
            'big-segments',
            'context-type',
            'filtering',
            'secure-mode-hash',
            'tags',
            'event-gzip',
            'optional-event-gzip',
            'event-sampling',
            'polling-gzip',
            'inline-context-all',
            'instance-id',
            'anonymous-redaction',
            'evaluation-hooks',
            'omit-anonymous-contexts',
            'client-prereq-events',
            'persistent-data-store-redis',
            'persistent-data-store-dynamodb',
            'persistent-data-store-consul',
            'flag-change-listeners',
            'flag-value-change-listeners',
            'fdv1-fallback',
            'migrations',
            'async',
        ]
    }
    return aiohttp.web.Response(
        text=json.dumps(body),
        content_type='application/json',
        status=200,
    )


async def handle_delete_stop(request: aiohttp.web.Request) -> aiohttp.web.Response:
    global_log.info("Test service has told us to exit")
    os._exit(0)


async def handle_create_client(request: aiohttp.web.Request) -> aiohttp.web.Response:
    global client_counter, clients

    try:
        options = await request.json()
    except Exception:
        return aiohttp.web.Response(text='Invalid JSON', status=400)

    client_counter += 1
    client_id = str(client_counter)
    resource_url = '/clients/%s' % client_id

    client = AsyncClientEntity(options['tag'], options['configuration'])
    try:
        await client.start()
    except Exception as e:
        global_log.exception(e)
        return aiohttp.web.Response(text=str(e), status=500)

    if not client.is_initializing() and not options['configuration'].get('initCanFail', False):
        await client.close()
        return aiohttp.web.Response(text='Failed to initialize', status=500)

    clients[client_id] = client
    return aiohttp.web.Response(status=201, headers={'Location': resource_url})


async def handle_client_command(request: aiohttp.web.Request) -> aiohttp.web.Response:
    global clients

    client_id = request.match_info['id']

    try:
        params = await request.json()
    except Exception:
        return aiohttp.web.Response(text='Invalid JSON', status=400)

    client = clients.get(client_id)
    if client is None:
        return aiohttp.web.Response(status=404)

    command = params.get('command')
    sub_params = params.get(command)

    response = None

    try:
        if command == "evaluate":
            response = await client.evaluate(sub_params)
        elif command == "evaluateAll":
            response = await client.evaluate_all(sub_params)
        elif command == "customEvent":
            client.track(sub_params)
        elif command == "identifyEvent":
            client.identify(sub_params)
        elif command == "flushEvents":
            await client.flush()
        elif command == "secureModeHash":
            response = client.secure_mode_hash(sub_params)
        elif command == "contextBuild":
            response = client.context_build(sub_params)
        elif command == "contextConvert":
            response = client.context_convert(sub_params)
        elif command == "getBigSegmentStoreStatus":
            response = await client.get_big_segment_store_status()
        elif command == "migrationVariation":
            response = await client.migration_variation(sub_params)
        elif command == "migrationOperation":
            response = await client.migration_operation(sub_params)
        elif command == "registerFlagChangeListener":
            await client.register_flag_change_listener(sub_params)
        elif command == "registerFlagValueChangeListener":
            await client.register_flag_value_change_listener(sub_params)
        elif command == "unregisterListener":
            success = await client.unregister_listener(sub_params)
            if not success:
                return aiohttp.web.Response(
                    text='no listener with id "%s"' % sub_params['listenerId'],
                    status=400,
                )
        else:
            return aiohttp.web.Response(status=400)
    except Exception as e:
        global_log.exception(e)
        return aiohttp.web.Response(text=str(e), status=500)

    if response is None:
        return aiohttp.web.Response(status=201)
    return aiohttp.web.Response(
        text=json.dumps(response),
        content_type='application/json',
        status=200,
    )


async def handle_delete_client(request: aiohttp.web.Request) -> aiohttp.web.Response:
    global clients

    client_id = request.match_info['id']
    client = clients.get(client_id)
    if client is None:
        return aiohttp.web.Response(status=404)

    await client.close()
    del clients[client_id]
    return aiohttp.web.Response(status=202)


# ---------------------------------------------------------------------------
# Helper functions (mirrors client_entity.py)
# ---------------------------------------------------------------------------

def _set_optional_time_prop(params_in: dict, name_in: str, params_out: dict, name_out: str):
    if params_in.get(name_in) is not None:
        params_out[name_out] = params_in[name_in] / 1000.0


def _set_optional_time(params_in: dict, name_in: str, func: Callable):
    if params_in.get(name_in) is not None:
        func(params_in[name_in] / 1000.0)


def _set_optional_value(params_in: dict, name_in: str, func: Callable):
    if params_in.get(name_in) is not None:
        func(params_in[name_in])


def _create_persistent_store(persistent_store_config: dict):
    """Creates a persistent store instance based on the configuration."""
    from urllib.parse import urlparse

    from ldclient.feature_store import CacheConfig
    from ldclient.integrations import Consul, DynamoDB, Redis

    store_params = persistent_store_config["store"]
    store_type = store_params["type"]
    dsn = store_params["dsn"]
    prefix = store_params.get("prefix")

    cache_config = persistent_store_config.get("cache", {})
    cache_mode = cache_config.get("mode", "ttl")

    if cache_mode == "off":
        caching = CacheConfig.disabled()
    elif cache_mode == "infinite":
        caching = CacheConfig(expiration=sys.maxsize)
    elif cache_mode == "ttl":
        ttl_seconds = cache_config.get("ttl", 15)
        caching = CacheConfig(expiration=ttl_seconds)
    else:
        caching = CacheConfig.default()

    if store_type == "redis":
        return Redis.new_feature_store(
            url=dsn,
            prefix=prefix or Redis.DEFAULT_PREFIX,
            caching=caching,
        )
    elif store_type == "dynamodb":
        parsed = urlparse(dsn) if '://' in dsn else urlparse(f'http://{dsn}')
        endpoint_url = f"{parsed.scheme}://{parsed.netloc}"
        import boto3
        dynamodb_opts = {
            'endpoint_url': endpoint_url,
            'region_name': 'us-east-1',
            'aws_access_key_id': 'dummy',
            'aws_secret_access_key': 'dummy',
        }
        return DynamoDB.new_feature_store(
            table_name="sdk-contract-tests",
            prefix=prefix,
            dynamodb_opts=dynamodb_opts,
            caching=caching,
        )
    elif store_type == "consul":
        parsed = urlparse(dsn) if '://' in dsn else urlparse(f'http://{dsn}')
        host = parsed.hostname or 'localhost'
        port = parsed.port or 8500
        return Consul.new_feature_store(
            host=host,
            port=port,
            prefix=prefix,
            caching=caching,
        )
    else:
        raise ValueError(f"Unsupported data store type: {store_type}")


# ---------------------------------------------------------------------------
# App factory and entry point
# ---------------------------------------------------------------------------

def create_app() -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    app.router.add_get('/', handle_status)
    app.router.add_delete('/', handle_delete_stop)
    app.router.add_post('/', handle_create_client)
    app.router.add_post('/clients/{id}', handle_client_command)
    app.router.add_delete('/clients/{id}', handle_delete_client)
    return app


if __name__ == "__main__":
    port = default_port
    if sys.argv[len(sys.argv) - 1] != 'async_service.py':
        port = int(sys.argv[len(sys.argv) - 1])
    global_log.info('Listening on port %d', port)
    app = create_app()
    aiohttp.web.run_app(app, host='0.0.0.0', port=port)
