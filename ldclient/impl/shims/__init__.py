"""
Async-only helpers backing the async data source, event processor, and data
system.

``shims.aio`` holds hand-maintained async concurrency helpers (``AsyncEvent``,
``AsyncQueue``, ``AsyncTaskRunner``, ``AsyncRepeatingTask``, ``AsyncWorkerPool``,
etc.) that wrap fiddly asyncio plumbing the async shells would otherwise inline
repeatedly. The sync shells use the equivalent stdlib/SDK primitives
(``threading``, ``queue.Queue``, ``RepeatingTask``, ``FixedThreadPool``,
urllib3) directly, so there is no sync twin for these.

``shims.aio_transport`` wraps an aiohttp ``ClientSession`` behind a
``TransportResponse`` (see ``shims.transport_types``) so async data-source
callers can inspect a response after the request context has closed and so the
SSL/session setup is written once. The sync side talks to urllib3 directly.

These are covered by ``ldclient/testing/impl/test_shims.py``.
"""
