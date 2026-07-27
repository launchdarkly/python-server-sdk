"""
Tests for AsyncHook dispatch in the evaluation pipeline.

The _evaluate_with_hooks helper defined here mirrors the dispatch pattern that
AsyncLDClient uses: async hooks are awaited, running in list order for
before_evaluation and reverse order for after_evaluation.

Each hook receives its own isolated data dict (matching the sync SDK pattern in
client.py:__execute_before_evaluation). A failing hook logs an error and returns
{} rather than aborting the evaluation (matching __try_execute_stage).
"""
import logging

import pytest

from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.hook import AsyncHook, EvaluationSeriesContext, Metadata

log = logging.getLogger('ldclient')


# ---------------------------------------------------------------------------
# Helpers: inline dispatch function that mirrors AsyncLDClient behaviour
# ---------------------------------------------------------------------------

async def _try_execute_stage_async(method, hook_name, coro_or_fn):
    """Execute a single hook stage, catching and logging any exceptions."""
    try:
        return await coro_or_fn()
    except BaseException as e:
        log.error(f"An error occurred in {method} of the hook {hook_name}: #{e}")
        return {}


async def _evaluate_with_hooks(hooks, series_context, eval_fn):
    """Dispatch before/after hooks around an async evaluation function.

    Each hook gets a fresh empty dict for before_evaluation (matching the sync
    SDK's __execute_before_evaluation pattern). The per-hook data returned is
    stored and passed back to the corresponding hook in after_evaluation.
    A hook that raises an exception is caught and logged; it does not abort
    the evaluation or affect other hooks.
    """
    # before_evaluation: each hook gets its own isolated {}
    hook_data = []
    for hook in hooks:
        data = await _try_execute_stage_async(
            "beforeEvaluation", hook.metadata.name,
            lambda h=hook: h.before_evaluation(series_context, {})
        )
        hook_data.append(data)

    detail = await eval_fn()

    # after_evaluation: reversed order, each hook receives its own before data
    for hook, data in reversed(list(zip(hooks, hook_data))):
        await _try_execute_stage_async(
            "afterEvaluation", hook.metadata.name,
            lambda h=hook, d=data: h.after_evaluation(series_context, d, detail)
        )

    return detail


# ---------------------------------------------------------------------------
# Concrete hook implementations for testing
# ---------------------------------------------------------------------------

class RecordingAsyncHook(AsyncHook):
    """Async hook that records calls and threads a counter through data."""

    def __init__(self, name: str):
        self._name = name
        self.before_calls: list = []
        self.after_calls: list = []

    @property
    def metadata(self) -> Metadata:
        return Metadata(name=self._name)

    async def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
        self.before_calls.append(series_context.key)
        return {**data, self._name + '_before': True}

    async def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict,
                               detail: EvaluationDetail) -> dict:
        self.after_calls.append(series_context.key)
        return {**data, self._name + '_after': True}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def series_context():
    return EvaluationSeriesContext(
        key='test-flag',
        context=Context.create('user-1'),
        default_value=False,
        method='variation',
    )


@pytest.fixture
def mock_detail():
    return EvaluationDetail(True, 0, {'kind': 'OFF'})


@pytest.fixture
def eval_fn(mock_detail):
    async def _fn():
        return mock_detail
    return _fn


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_hook_before_and_after_awaited(series_context, eval_fn):
    hook = RecordingAsyncHook('async1')
    await _evaluate_with_hooks([hook], series_context, eval_fn)

    assert hook.before_calls == ['test-flag']
    assert hook.after_calls == ['test-flag']


@pytest.mark.asyncio
async def test_hooks_called_in_order(series_context, eval_fn):
    """before_evaluation runs in list order; after_evaluation runs in reverse."""
    call_log = []

    class OrderAsyncHook(AsyncHook):
        def __init__(self, name):
            self._name = name

        @property
        def metadata(self):
            return Metadata(name=self._name)

        async def before_evaluation(self, sc, data):
            call_log.append(self._name + '_before')
            return data

        async def after_evaluation(self, sc, data, detail):
            call_log.append(self._name + '_after')
            return data

    hooks = [OrderAsyncHook('first'), OrderAsyncHook('second')]
    await _evaluate_with_hooks(hooks, series_context, eval_fn)

    # before: list order (first, second)
    # after: reversed (second, first)
    assert call_log == ['first_before', 'second_before', 'second_after', 'first_after']


@pytest.mark.asyncio
async def test_hooks_each_called_with_series_context(series_context, eval_fn):
    """Each async hook is invoked with the correct series context."""
    hook_a = RecordingAsyncHook('a')
    hook_b = RecordingAsyncHook('b')

    hooks = [hook_a, hook_b]
    await _evaluate_with_hooks(hooks, series_context, eval_fn)

    assert hook_a.before_calls == ['test-flag']
    assert hook_b.before_calls == ['test-flag']


@pytest.mark.asyncio
async def test_returns_evaluation_detail(series_context, mock_detail, eval_fn):
    hook = RecordingAsyncHook('a')
    result = await _evaluate_with_hooks([hook], series_context, eval_fn)
    assert result is mock_detail


@pytest.mark.asyncio
async def test_no_hooks_returns_detail(series_context, mock_detail, eval_fn):
    result = await _evaluate_with_hooks([], series_context, eval_fn)
    assert result is mock_detail


@pytest.mark.asyncio
async def test_data_isolated_per_hook(series_context, eval_fn):
    """Each hook receives its own fresh {} for before_evaluation (matching sync SDK pattern).

    Data is not threaded across hooks — each hook's before_evaluation starts
    with an empty dict and the result is passed only to that hook's own
    after_evaluation. This matches client.py:__execute_before_evaluation.
    """
    before_data_received = {}

    class CaptureAsyncHook(AsyncHook):
        def __init__(self, name):
            self._name = name

        @property
        def metadata(self):
            return Metadata(name=self._name)

        async def before_evaluation(self, sc, data):
            before_data_received[self._name] = dict(data)
            return {**data, self._name + '_key': 1}

        async def after_evaluation(self, sc, data, detail):
            return data

    await _evaluate_with_hooks([CaptureAsyncHook('one'), CaptureAsyncHook('two')], series_context, eval_fn)

    # Each hook should have received a fresh empty dict, not the other hook's output
    assert before_data_received['one'] == {}
    assert before_data_received['two'] == {}
