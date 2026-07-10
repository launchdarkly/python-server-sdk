"""
Async support classes backing the async data source, event processor, and data
system. These have no sync twin: the sync code uses the equivalent stdlib/SDK
primitives (``threading``, ``queue.Queue``, ``RepeatingTask``,
``FixedThreadPool``, urllib3) directly.

``aio.concurrency`` holds the async concurrency primitives (``AsyncEvent``,
``AsyncQueue``, ``AsyncTaskRunner``, ``AsyncRepeatingTask``, ``AsyncWorkerPool``,
etc.) that wrap fiddly asyncio plumbing the async code would otherwise inline
repeatedly.

``aio.transport`` wraps an aiohttp ``ClientSession`` behind a
``TransportResponse`` (see ``aio.transport_types``) so async data-source callers
can inspect a response after the request context has closed and so the
SSL/session setup is written once. The sync side talks to urllib3 directly.

These are covered by ``ldclient/testing/test_aio.py``.
"""
