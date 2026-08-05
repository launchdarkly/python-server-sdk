"""
Tests for the async concurrency primitives (``aio.concurrency``) and the async
transport (``aio.transport``) in ldclient/impl/aio/. The sync side uses
stdlib/SDK primitives directly, so there is no sync twin to assert parity
against.
"""

import asyncio
import subprocess
import sys
import threading
import time

import pytest

from ldclient.config import Config, HTTPConfig
from ldclient.impl.aio import concurrency as aio
from ldclient.impl.aio.transport import AsyncHTTPTransport, AsyncSSEFactory
from ldclient.testing.http_util import (
    BasicResponse,
    JsonResponse,
    start_server
)


async def _async_wait_until(predicate, timeout=2.0):
    deadline = time.time() + timeout
    while not predicate():
        assert time.time() < deadline, "timed out waiting for condition"
        await asyncio.sleep(0.01)


class TestEventParity:
    @pytest.mark.asyncio
    async def test_async_set_clear_is_set(self):
        event = aio.AsyncEvent()
        assert not event.is_set()
        event.set()
        assert event.is_set()
        assert await event.wait(1) is True
        event.clear()
        assert not event.is_set()

    @pytest.mark.asyncio
    async def test_async_wait_timeout_returns_false(self):
        event = aio.AsyncEvent()
        start = time.time()
        assert await event.wait(0.05) is False
        assert time.time() - start >= 0.04

    @pytest.mark.asyncio
    async def test_async_wait_wakes_when_set(self):
        event = aio.AsyncEvent()

        async def setter():
            await asyncio.sleep(0.02)
            event.set()

        task = asyncio.ensure_future(setter())
        assert await event.wait(2) is True
        await task


class TestLockParity:
    @pytest.mark.asyncio
    async def test_async_lock_context_manager(self):
        lock = aio.AsyncLock()
        assert not lock.locked()
        async with lock:
            assert lock.locked()
        assert not lock.locked()

    @pytest.mark.asyncio
    async def test_async_lock_mutual_exclusion(self):
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


class TestQueueParity:
    @pytest.mark.asyncio
    async def test_async_put_get_and_empty(self):
        q = aio.AsyncQueue()
        assert q.empty()
        await q.put('a')
        assert not q.empty()
        assert await q.get(1) == 'a'
        assert q.empty()

    @pytest.mark.asyncio
    async def test_async_get_timeout_raises_queue_empty(self):
        q = aio.AsyncQueue()
        with pytest.raises(aio.QueueEmpty):
            await q.get(0.05)

    @pytest.mark.asyncio
    async def test_async_get_nonblocking_raises_queue_empty(self):
        q = aio.AsyncQueue()
        with pytest.raises(aio.QueueEmpty):
            await q.get(block=False)

    @pytest.mark.asyncio
    async def test_async_put_nowait_raises_queue_full_at_capacity(self):
        q = aio.AsyncQueue(1)
        q.put_nowait('a')
        with pytest.raises(aio.QueueFull):
            q.put_nowait('b')
        assert await q.get(block=False) == 'a'


class TestRepeatingTaskParity:
    @pytest.mark.asyncio
    async def test_async_fires_repeatedly_then_stops(self):
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
    async def test_async_initial_delay_respected(self):
        counts = {'n': 0}

        async def action():
            counts['n'] += 1

        task = aio.AsyncRepeatingTask("test.repeating", 0.01, 0.1, action)
        task.start()
        await asyncio.sleep(0.03)
        assert counts['n'] == 0
        task.stop()

    @pytest.mark.asyncio
    async def test_async_continues_after_action_exception(self):
        counts = {'n': 0}

        async def action():
            counts['n'] += 1
            raise RuntimeError("boom")

        task = aio.AsyncRepeatingTask("test.repeating", 0.01, 0, action)
        task.start()
        await _async_wait_until(lambda: counts['n'] >= 2)
        task.stop()

    @pytest.mark.asyncio
    async def test_async_stop_from_within_action(self):
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
    async def test_async_second_start_raises(self):
        async def action():
            pass

        task = aio.AsyncRepeatingTask("test.repeating", 0.01, 0, action)
        task.start()
        with pytest.raises(RuntimeError):
            task.start()
        task.stop()


class TestBoundedTaskSet:
    @pytest.mark.asyncio
    async def test_saturation_returns_false(self):
        tasks = aio.BoundedTaskSet(1)
        release = aio.AsyncEvent()
        started = aio.AsyncEvent()

        async def job():
            started.set()
            await release.wait(2)

        async def noop():
            pass

        assert tasks.try_run(job) is True
        await started.wait(2)
        # Set is full (limit 1), so the next task is rejected rather than queued.
        assert tasks.try_run(noop) is False
        release.set()
        await tasks.wait()

        # A slot is free again once the first task finished.
        assert tasks.try_run(noop) is True
        await tasks.wait()

        # After stop(), further work is rejected.
        tasks.stop()
        assert tasks.try_run(noop) is False

    @pytest.mark.asyncio
    async def test_wait_returns_when_idle(self):
        tasks = aio.BoundedTaskSet(2)
        await tasks.wait()
        tasks.stop()

    @pytest.mark.asyncio
    async def test_retry_pattern_makes_progress_when_task_finished_in_same_batch(self):
        tasks = aio.BoundedTaskSet(1)
        finished = asyncio.Event()

        async def job():
            finished.set()

        assert tasks.try_run(job) is True

        # A raw asyncio.Event wakes this coroutine in the same loop batch the
        # job's task finished in, before try_run sees the slot freed.
        await finished.wait()

        async def noop():
            pass

        # Mirrors the retry loop in EventDispatcher._run_main_loop: try_run must
        # free the finished task's slot, or the real (unbounded) loop spins.
        accepted = False
        for _ in range(100):
            if tasks.try_run(noop):
                accepted = True
                break
            await tasks.wait()

        assert accepted, (
            "try_run never freed capacity: the finished task was still counted "
            "against the limit, so the dispatcher's retry loop would spin forever"
        )
        await tasks.wait()


class TestTaskRunnerParity:
    @pytest.mark.asyncio
    async def test_async_spawn_runs_function(self):
        runner = aio.AsyncTaskRunner()
        done = aio.AsyncEvent()

        async def fn():
            done.set()

        runner.spawn("test.task", fn)
        assert await done.wait(2)
        await runner.stop_all()
        assert runner.is_stopped()

    @pytest.mark.asyncio
    async def test_async_stop_all_cancels_running_tasks(self):
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
    async def test_async_stop_all_accepts_timeout(self):
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


class TestHandleParity:
    @pytest.mark.asyncio
    async def test_async_spawn_and_join(self):
        done = aio.AsyncEvent()

        async def fn():
            done.set()

        handle = aio.spawn_handle("test.handle", fn)
        await aio.join_handle(handle, 2)
        assert done.is_set()
        assert handle.done()

    @pytest.mark.asyncio
    async def test_async_join_times_out_and_cancels(self):
        started = aio.AsyncEvent()

        async def slow():
            started.set()
            await asyncio.sleep(60)

        handle = aio.spawn_handle("test.slow", slow)
        await started.wait(2)
        await aio.join_handle(handle, 0.1)
        await _async_wait_until(handle.done)
        assert handle.cancelled()

    @pytest.mark.asyncio
    async def test_async_join_swallows_task_exception(self):
        async def boom():
            raise ValueError("boom")

        handle = aio.spawn_handle("test.boom", boom)
        await aio.join_handle(handle, 2)  # does not raise

    @pytest.mark.asyncio
    async def test_async_join_propagates_caller_cancellation(self):
        # If the joining task is cancelled while parked in join_handle, the
        # cancellation must propagate -- even when the joined task is *also*
        # cancelled first. A `try/except CancelledError: return` keyed off
        # handle.cancelled() would swallow the caller's cancellation here, so
        # this guards against reintroducing that pattern.
        started = aio.AsyncEvent()

        async def slow():
            started.set()
            await asyncio.sleep(60)

        handle = aio.spawn_handle("test.slow", slow)
        await started.wait(2)

        joiner = asyncio.ensure_future(aio.join_handle(handle, 30))
        await asyncio.sleep(0.05)  # let the joiner park inside join_handle
        handle.cancel()            # joined task cancelled first...
        joiner.cancel()            # ...then the joiner, while still parked

        with pytest.raises(asyncio.CancelledError):
            await joiner


class TestCallbackSchedulerParity:
    @pytest.mark.asyncio
    async def test_async_call_schedules_coroutine_with_args(self):
        scheduler = aio.AsyncCallbackScheduler()
        received = []

        async def cb(value):
            received.append(value)

        scheduler.call(cb, 'value')
        await _async_wait_until(lambda: received == ['value'])

    @pytest.mark.asyncio
    async def test_async_call_works_from_worker_thread(self):
        scheduler = aio.AsyncCallbackScheduler()
        received = []

        async def cb(value):
            received.append(value)

        thread = threading.Thread(target=lambda: scheduler.call(cb, 'threaded'))
        thread.start()
        thread.join()
        await _async_wait_until(lambda: received == ['threaded'])

    @pytest.mark.asyncio
    async def test_async_call_logs_callback_exception(self):
        scheduler = aio.AsyncCallbackScheduler()
        completed = aio.AsyncEvent()

        async def boom():
            completed.set()
            raise ValueError("boom")

        scheduler.call(boom)  # must not raise
        assert await completed.wait(2)
        await asyncio.sleep(0.05)  # let the done callback run


class _FakeResponse:
    status = 200
    headers: dict = {}

    async def text(self, **kwargs):
        return ""


class _FakeResponseCtx:
    async def __aenter__(self):
        return _FakeResponse()

    async def __aexit__(self, *exc):
        return False


class _RecordingSession:
    """A stand-in aiohttp session that records each request's kwargs (so tests
    can assert the proxy passed) and yields a canned response."""

    def __init__(self):
        self.calls = []

    def request(self, method, uri, **kwargs):
        self.calls.append(kwargs)
        return _FakeResponseCtx()

    async def close(self):
        pass


class TestTransportParity:
    @pytest.mark.asyncio
    async def test_async_get(self):
        with start_server() as server:
            config = Config(sdk_key='sdk-key', base_uri=server.uri)
            transport = AsyncHTTPTransport(config)
            server.for_path('/path', JsonResponse({'hello': 'world'}, {'Etag': 'my-etag'}))

            resp = await transport.request('GET', server.uri + '/path', headers={'X-Test': 'yes'})

            assert resp.status == 200
            assert resp.headers.get('ETag') == 'my-etag'
            assert '"hello"' in resp.body
            req = server.require_request()
            assert req.headers['X-Test'] == 'yes'
            await transport.close()

    @pytest.mark.asyncio
    async def test_async_post_with_body(self):
        with start_server() as server:
            config = Config(sdk_key='sdk-key', base_uri=server.uri)
            transport = AsyncHTTPTransport(config)
            server.for_path('/events', BasicResponse(202))

            resp = await transport.request('POST', server.uri + '/events', headers={'Content-Type': 'application/json'}, body='[{"kind":"custom"}]')

            assert resp.status == 202
            req = server.require_request()
            assert req.method == 'POST'
            assert req.body == '[{"kind":"custom"}]'
            await transport.close()

    @pytest.mark.asyncio
    async def test_async_error_status_is_returned_not_raised(self):
        with start_server() as server:
            config = Config(sdk_key='sdk-key', base_uri=server.uri)
            transport = AsyncHTTPTransport(config)
            server.for_path('/missing', BasicResponse(404))

            resp = await transport.request('GET', server.uri + '/missing')
            assert resp.status == 404
            await transport.close()

    # Proxy handling must match the sync SDK (ldclient.impl.http): a configured
    # proxy wins, otherwise fall back to the standard proxy env vars with
    # NO_PROXY rules applied per target URI. (Guards against reverting to
    # aiohttp's trust_env, whose env/NO_PROXY handling differs.)

    @pytest.mark.asyncio
    async def test_request_uses_configured_proxy(self, monkeypatch):
        monkeypatch.setenv('https_proxy', 'http://env-proxy:8080')
        session = _RecordingSession()
        config = Config('sdk-key', http=HTTPConfig(http_proxy='http://cfg-proxy:9000'))
        transport = AsyncHTTPTransport(config, client=session)
        await transport.request('GET', 'https://x.launchdarkly.com/p')
        assert session.calls[0]['proxy'] == 'http://cfg-proxy:9000'

    @pytest.mark.asyncio
    async def test_request_falls_back_to_env_proxy(self, monkeypatch):
        monkeypatch.setenv('https_proxy', 'http://env-proxy:8080')
        monkeypatch.delenv('no_proxy', raising=False)
        session = _RecordingSession()
        transport = AsyncHTTPTransport(Config('sdk-key'), client=session)
        await transport.request('GET', 'https://x.launchdarkly.com/p')
        assert session.calls[0]['proxy'] == 'http://env-proxy:8080'

    @pytest.mark.asyncio
    async def test_request_honors_no_proxy(self, monkeypatch):
        monkeypatch.setenv('https_proxy', 'http://env-proxy:8080')
        monkeypatch.setenv('no_proxy', 'launchdarkly.com')
        session = _RecordingSession()
        transport = AsyncHTTPTransport(Config('sdk-key'), client=session)
        await transport.request('GET', 'https://x.launchdarkly.com/p')
        assert session.calls[0]['proxy'] is None

    def test_sse_factory_proxy_precedence(self):
        # Explicit proxy arg overrides the configured proxy.
        cfg = Config('sdk-key', http=HTTPConfig(http_proxy='http://cfg-proxy:9000'))
        assert AsyncSSEFactory(cfg, proxy='http://override:1')._proxy == 'http://override:1'
        # Otherwise the configured proxy is used.
        assert AsyncSSEFactory(cfg)._proxy == 'http://cfg-proxy:9000'
        # Neither set -> None (per-URL env fallback happens in create()).
        assert AsyncSSEFactory(Config('sdk-key'))._proxy is None


class TestImportSafety:
    def test_import_ldclient_does_not_require_aiohttp(self):
        """A bare ``import ldclient`` must not import anything that requires an
        optional dependency. aiohttp (the ``async`` extra) is optional, so a
        sync-only install must still be able to import ldclient.
        """
        code = "import ldclient, sys; sys.exit(1 if 'aiohttp' in sys.modules else 0)"
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, (
            "importing ldclient pulled in aiohttp. Keep aiohttp off the eager "
            "import path (import async_client / transport lazily).\n"
            "stderr:\n%s" % result.stderr
        )
