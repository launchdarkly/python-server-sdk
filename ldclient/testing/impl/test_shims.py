"""
Tests for the async concurrency and HTTP transport helpers in
``ldclient/impl/shims/``. ``shims.aio`` holds the async concurrency
primitives; ``shims.aio_transport`` wraps an aiohttp session for HTTP and SSE.
"""

import asyncio
import ssl
import threading
import time

import aiohttp
import pytest
from ld_eventsource.async_client import AsyncSSEClient

from ldclient.config import Config, HTTPConfig
from ldclient.impl.shims import aio
from ldclient.impl.shims.aio_transport import (
    AsyncHTTPTransport,
    AsyncSSEFactory,
    make_client_session
)


async def _async_wait_until(predicate, timeout=2.0):
    deadline = time.time() + timeout
    while not predicate():
        assert time.time() < deadline, "timed out waiting for condition"
        await asyncio.sleep(0.01)


# ---------------------------------------------------------------------------
# aio.AsyncEvent
# ---------------------------------------------------------------------------

class TestAsyncEvent:
    @pytest.mark.asyncio
    async def test_set_clear_is_set(self):
        event = aio.AsyncEvent()
        assert not event.is_set()
        event.set()
        assert event.is_set()
        assert await event.wait(1) is True
        event.clear()
        assert not event.is_set()

    @pytest.mark.asyncio
    async def test_wait_timeout_returns_false(self):
        event = aio.AsyncEvent()
        start = time.time()
        assert await event.wait(0.05) is False
        assert time.time() - start >= 0.04

    @pytest.mark.asyncio
    async def test_wait_wakes_when_set(self):
        event = aio.AsyncEvent()

        async def setter():
            await asyncio.sleep(0.02)
            event.set()

        task = asyncio.ensure_future(setter())
        assert await event.wait(2) is True
        await task


# ---------------------------------------------------------------------------
# aio.AsyncLock
# ---------------------------------------------------------------------------

class TestAsyncLock:
    @pytest.mark.asyncio
    async def test_context_manager_tracks_locked_state(self):
        lock = aio.AsyncLock()
        assert not lock.locked()
        async with lock:
            assert lock.locked()
        assert not lock.locked()

    @pytest.mark.asyncio
    async def test_mutual_exclusion(self):
        lock = aio.AsyncLock()
        counter = {'value': 0, 'concurrent': 0, 'max_concurrent': 0}

        async def work():
            async with lock:
                counter['concurrent'] += 1
                counter['max_concurrent'] = max(counter['max_concurrent'], counter['concurrent'])
                await asyncio.sleep(0.01)
                counter['value'] += 1
                counter['concurrent'] -= 1

        await asyncio.gather(*(work() for _ in range(5)))
        assert counter['value'] == 5
        assert counter['max_concurrent'] == 1


# ---------------------------------------------------------------------------
# aio.AsyncRepeatingTask
# ---------------------------------------------------------------------------

class TestAsyncRepeatingTask:
    @pytest.mark.asyncio
    async def test_fires_repeatedly_then_stops_cleanly(self):
        counts = {'n': 0}

        async def action():
            counts['n'] += 1

        task = aio.AsyncRepeatingTask("test.repeating", 0.01, 0, action)
        task.start()
        await _async_wait_until(lambda: counts['n'] >= 3)
        task.stop()
        await asyncio.sleep(0.05)
        snapshot = counts['n']
        await asyncio.sleep(0.05)
        assert counts['n'] == snapshot

    @pytest.mark.asyncio
    async def test_initial_delay_respected(self):
        counts = {'n': 0}

        async def action():
            counts['n'] += 1

        task = aio.AsyncRepeatingTask("test.repeating", 0.01, 0.1, action)
        task.start()
        await asyncio.sleep(0.03)
        assert counts['n'] == 0
        task.stop()

    @pytest.mark.asyncio
    async def test_continues_after_action_exception(self):
        counts = {'n': 0}

        async def action():
            counts['n'] += 1
            raise RuntimeError("boom")

        task = aio.AsyncRepeatingTask("test.repeating", 0.01, 0, action)
        task.start()
        await _async_wait_until(lambda: counts['n'] >= 2)
        task.stop()

    @pytest.mark.asyncio
    async def test_stop_from_within_action(self):
        counts = {'n': 0}
        holder = {}

        async def action():
            counts['n'] += 1
            holder['task'].stop()

        holder['task'] = aio.AsyncRepeatingTask("test.repeating", 0.01, 0, action)
        holder['task'].start()
        await asyncio.sleep(0.1)
        assert counts['n'] == 1

    @pytest.mark.asyncio
    async def test_second_start_raises(self):
        async def action():
            pass

        task = aio.AsyncRepeatingTask("test.repeating", 0.01, 0, action)
        task.start()
        with pytest.raises(RuntimeError):
            task.start()
        task.stop()


# ---------------------------------------------------------------------------
# aio.AsyncCallbackScheduler
# ---------------------------------------------------------------------------

class TestAsyncCallbackScheduler:
    @pytest.mark.asyncio
    async def test_call_schedules_coroutine_with_args(self):
        scheduler = aio.AsyncCallbackScheduler()
        received = []

        async def cb(value):
            received.append(value)

        scheduler.call(cb, 'value')
        await _async_wait_until(lambda: received == ['value'])

    @pytest.mark.asyncio
    async def test_call_works_from_worker_thread(self):
        scheduler = aio.AsyncCallbackScheduler()
        received = []

        async def cb(value):
            received.append(value)

        thread = threading.Thread(target=lambda: scheduler.call(cb, 'threaded'))
        thread.start()
        thread.join()
        await _async_wait_until(lambda: received == ['threaded'])

    @pytest.mark.asyncio
    async def test_call_swallows_callback_exception(self):
        scheduler = aio.AsyncCallbackScheduler()
        completed = aio.AsyncEvent()

        async def boom():
            completed.set()
            raise ValueError("boom")

        scheduler.call(boom)  # must not raise
        assert await completed.wait(2)
        await asyncio.sleep(0.05)  # let the done callback run


# ---------------------------------------------------------------------------
# aio.AsyncTaskRunner
# ---------------------------------------------------------------------------

class TestAsyncTaskRunner:
    @pytest.mark.asyncio
    async def test_spawn_runs_function(self):
        runner = aio.AsyncTaskRunner()
        done = aio.AsyncEvent()

        async def fn():
            done.set()

        runner.spawn("test.task", fn)
        assert await done.wait(2)
        await runner.stop_all()
        assert runner.is_stopped()

    @pytest.mark.asyncio
    async def test_stop_all_cancels_running_tasks(self):
        runner = aio.AsyncTaskRunner()
        started = aio.AsyncEvent()

        async def forever():
            started.set()
            await asyncio.sleep(60)

        task = runner.spawn("test.forever", forever)
        await started.wait(2)
        await runner.stop_all()
        assert task.done()

    @pytest.mark.asyncio
    async def test_stop_all_honors_timeout_for_stubborn_task(self):
        runner = aio.AsyncTaskRunner()
        started = aio.AsyncEvent()
        give_up = asyncio.Event()

        async def stubborn():
            while not give_up.is_set():
                try:
                    started.set()
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    continue  # refuse to die until give_up is set

        task = runner.spawn("test.stubborn", stubborn)
        await started.wait(2)
        start = time.time()
        await runner.stop_all(timeout=0.1)
        assert time.time() - start < 2
        assert not task.done()  # the stubborn task outlived stop_all
        give_up.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# aio.resolve
# ---------------------------------------------------------------------------

class TestResolve:
    @pytest.mark.asyncio
    async def test_resolve_plain_value(self):
        assert await aio.resolve(42) == 42

    @pytest.mark.asyncio
    async def test_resolve_awaitable(self):
        async def coro():
            return 42

        assert await aio.resolve(coro()) == 42


# ---------------------------------------------------------------------------
# aio_transport.make_client_session
# ---------------------------------------------------------------------------

class TestMakeClientSession:
    @pytest.mark.asyncio
    async def test_default_verifies_tls_and_trusts_env(self):
        config = Config(sdk_key='sdk-key')
        session = make_client_session(config)
        try:
            ctx = session.connector._ssl
            assert isinstance(ctx, ssl.SSLContext)
            assert ctx.check_hostname is True
            assert ctx.verify_mode == ssl.CERT_REQUIRED
            assert session.trust_env is True
        finally:
            await session.close()

    @pytest.mark.asyncio
    async def test_disable_ssl_verification_relaxes_context(self):
        config = Config(sdk_key='sdk-key', http=HTTPConfig(disable_ssl_verification=True))
        session = make_client_session(config)
        try:
            ctx = session.connector._ssl
            assert ctx.check_hostname is False
            assert ctx.verify_mode == ssl.CERT_NONE
        finally:
            await session.close()

    @pytest.mark.asyncio
    async def test_proxy_disables_env_trust(self):
        config = Config(sdk_key='sdk-key', http=HTTPConfig(http_proxy='http://my-proxy:1234'))
        session = make_client_session(config)
        try:
            assert session.trust_env is False
        finally:
            await session.close()

    @pytest.mark.asyncio
    async def test_http_options_override_config(self):
        config = Config(sdk_key='sdk-key')
        override = HTTPConfig(disable_ssl_verification=True)
        session = make_client_session(config, http_options=override)
        try:
            ctx = session.connector._ssl
            assert ctx.verify_mode == ssl.CERT_NONE
        finally:
            await session.close()


# ---------------------------------------------------------------------------
# aio_transport.AsyncHTTPTransport ownership
# ---------------------------------------------------------------------------

class TestAsyncHTTPTransportOwnership:
    @pytest.mark.asyncio
    async def test_closes_session_it_created(self):
        config = Config(sdk_key='sdk-key')
        transport = AsyncHTTPTransport(config)
        # Force lazy session creation without making a network request.
        transport._client = make_client_session(config)
        transport._owns_client = True
        session = transport._client
        await transport.close()
        assert session.closed is True
        assert transport._client is None

    @pytest.mark.asyncio
    async def test_leaves_caller_supplied_session_open(self):
        config = Config(sdk_key='sdk-key')
        session = make_client_session(config)
        try:
            transport = AsyncHTTPTransport(config, client=session)
            await transport.close()
            assert session.closed is False
        finally:
            await session.close()


# ---------------------------------------------------------------------------
# aio_transport.AsyncSSEFactory
# ---------------------------------------------------------------------------

class TestAsyncSSEFactory:
    @pytest.mark.asyncio
    async def test_create_with_supplied_session_returns_client_and_leaves_it_open(self):
        config = Config(sdk_key='sdk-key')
        session = make_client_session(config)
        try:
            factory = AsyncSSEFactory(config, session=session)
            client = factory.create('http://localhost:1/stream', 1.0)
            assert isinstance(client, AsyncSSEClient)
            assert session.closed is False
        finally:
            await session.close()

    @pytest.mark.asyncio
    async def test_create_without_session_returns_client(self):
        config = Config(sdk_key='sdk-key')
        factory = AsyncSSEFactory(config)
        client = factory.create('http://localhost:1/stream', 1.0)
        assert isinstance(client, AsyncSSEClient)
