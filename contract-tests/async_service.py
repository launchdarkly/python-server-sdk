import json
import logging
import os
import sys
from logging.config import dictConfig
from typing import Any, Dict

# Import ldclient from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import aiohttp.web  # noqa: E402
from async_client_entity import AsyncClientEntity  # noqa: E402

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
            'flag-change-listeners',
            'flag-value-change-listeners',
            'migrations',
            'persistent-data-store-redis',
            'fdv1-fallback',
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
        # Close the partially-started client so it does not leak tasks or sessions.
        await client.close()
        return aiohttp.web.Response(text=str(e), status=500)

    if not await client.is_initializing() and not options['configuration'].get('initCanFail', False):
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
