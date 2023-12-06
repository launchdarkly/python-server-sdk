import uvicorn
from fastapi import Request, Response, FastAPI, status as faststatus
from fastapi.responses import JSONResponse
import os
import json
from client_entity import ClientEntity
import logging

client_counter = 0
clients = {}
app = FastAPI()
global_log = logging.getLogger('testservice')


# @app.errorhandler(Exception)
# def handle_exception(e):
#     # pass through HTTP errors
#     if isinstance(e, HTTPException):
#         return e
#
#     app.logger.exception(e)
#     return str(e), 500


@app.get('/')
async def status():
    body = {
        'capabilities': [
            'server-side',
            'all-flags-with-reasons',
            'all-flags-client-side-only',
            'all-flags-details-only-for-tracked-flags',
            'big-segments',
            'context-type',
            'secure-mode-hash',
            'tags',
            'migrations',
            'event-sampling',
            'server-side-polling'
        ]
    }
    return body


@app.delete('/')
async def delete_stop_service():
    global_log.info("Test service has told us to exit")
    os._exit(0)


@app.post('/')
async def post_create_client(request: Request, response: Response):
    global client_counter, clients

    options = await request.json()

    client_counter += 1
    client_id = str(client_counter)
    resource_url = '/clients/%s' % client_id

    client = ClientEntity(options['tag'], options['configuration'])
    await client.client.wait_for_initialization((options['configuration'].get("startWaitTimeMs") or 5_000) / 1_000.0)
    if client.is_initializing() is False and options['configuration'].get('initCanFail', False) is False:
        client.close()
        response.status_code = faststatus.HTTP_500_INTERNAL_SERVER_ERROR
        return "Failed to initialize"

    clients[client_id] = client
    response.headers['Location'] = resource_url
    response.status_code = faststatus.HTTP_201_CREATED
    return ''


@app.post('/clients/{id}')
async def post_client_command(id, request: Request, response: Response):
    global clients

    params = await request.json()

    client = clients[id]
    if client is None:
        return ('', 404)

    command = params.get('command')
    sub_params = params.get(command)

    command_response = None

    if command == "evaluate":
        command_response = await client.evaluate(sub_params)
    elif command == "evaluateAll":
        command_response = await client.evaluate_all(sub_params)
    elif command == "customEvent":
        client.track(sub_params)
    elif command == "identifyEvent":
        client.identify(sub_params)
    elif command == "flushEvents":
        client.flush()
    elif command == "secureModeHash":
        command_response = client.secure_mode_hash(sub_params)
    elif command == "contextBuild":
        command_response = client.context_build(sub_params)
    elif command == "contextConvert":
        command_response = client.context_convert(sub_params)
    elif command == "getBigSegmentStoreStatus":
        command_response = client.get_big_segment_store_status()
    elif command == "migrationVariation":
        command_response = await client.migration_variation(sub_params)
    elif command == "migrationOperation":
        command_response = await client.migration_operation(sub_params)
    else:
        response.status_code = faststatus.HTTP_400_BAD_REQUEST
        return ''

    if command_response is None:
        response.status_code = faststatus.HTTP_201_CREATED
        return ''
    return command_response


@app.delete('/clients/{id}')
async def delete_client(id, response: Response):
    global clients

    client = clients[id]
    if client is None:
        response.status_code = faststatus.HTTP_404_NOT_FOUND
        return ''

    await client.close()
    response.status_code = faststatus.HTTP_202_ACCEPTED
    return ''

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
